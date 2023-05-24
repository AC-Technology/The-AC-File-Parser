import pandas as pd
import requests
import json
# import boto3
import io

def main():
    filename = 'Nationwide(All Codes) - January 2022'

    # print(event)
    # key = event['params']['querystring']['key']
    # print(key)
    # filename = event['params']['querystring']['filename']
    # dot = filename.index(".")
    # filename = filename[:dot]

    client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
    client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
    refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'
    
    # this is the authorization url. As you can see it is just an endpoint (URL) that uses the above variables to retrieve an access token.
    # Fundamentally, what an API call does in any language (python, javascript), is put the info in the correct place in the URL, access it, and return a response
    # based on what has been programmed on the other side of the API to respond.
    
    auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
        '&grant_type=refresh_token&client_id=' + \
        client_id+'&client_secret=' + client_secret
    
    
    # these are the urls that will be used to upload data into different modules via the API. As you can see, the end
    # of the urls are just the API Names of the module you are inserting to. To find the api names of different modules,
    # go to the settings of the CRM > Developer Space > APIs > API Names
    
    # Big picture terms: uploading data into the system == sending data to these urls.
    
    consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
    agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'
    agency_code_search_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes/search'
    member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'



    # This part of our code retrieves the access token
    # using the built-in request library for making http requests, we make a post request to the auth url to retrieve an access token.
    auth_response = requests.post(url=auth_url)

    # print(auth_response.json())

    # turn the response into readable json and store the access token in a variable
    access_token = auth_response.json()['access_token']
    # print(access_token)

    # this authorization header will be used to make API calls. This is specified in the Zoho CRM documentation.
    # https://www.zoho.com/crm/developer/docs/api/v2/insert-records.html
    # it would be helpful to have this open in a tab.

    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    #Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    word_date = list_words[3]
    space = list_words[3].index(" ")
    month = list_words[3][:3]
    year = list_words[3][space:]
    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)
        
        
    if "All In" in filename:
        Name = 'Nationwide(All In) - ' + word_date
    elif "Elite Contract" in filename:
        Name = "Nationwide(Elite Contract) - " + word_date
    elif "Standard Contract" in filename:
        Name = "Nationwide(Standard Contract) - " + word_date
    elif "All Codes" in filename:
        Name = "Nationwide(All Codes) - " + word_date

    table1 = pd.read_csv("table_bb4da6c9ab0143beba14a432ec770753_page_1.csv")
    table2 = pd.read_csv("table_3e00133b56b84dd3a6c6afb6c5eeca3d_page_1.csv")
    table3 = pd.read_csv("table_e53c7d908acb470794a72d40937f8914_page_1.csv")

    # create a carrrier report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'

    Prior_YTD_DWP = currency_format(table1['Prior YTD DWP'].iloc[-1])
    YTD_DWP = currency_format(table1['Curr. YTD DWP'].iloc[-1])
    YTD_DWP_Growth = currency_format(table1['YTD DWP Growth'].iloc[-1])
    DWP_12MM = currency_format(table1['12MM DWP'].iloc[-1])
    Prior_YTD_NB_DWP = currency_format(table1['Prior YTD NB DWP'].iloc[-1])
    YTD_NB_DWP = currency_format(table1['Curr. YTD NB DWP'].iloc[-1])
    YTD_PIF = currency_format(table2['Curr. PIF'].iloc[-2])
    YTD_NB_PIF =  currency_format(table2['Curr. YTD NB PIF'].iloc[-2])
    Prior_YTD_NB_PIF = currency_format(table2['Prior YTD NB PIF'].iloc[-2])
    Prior_YTD_PIF = currency_format(table2['Prior YTD PIF'].iloc[-2])
    Loss_Ratio_12MM = currency_format(table3['12MM LR'].iloc[-1])
    YTD_Loss_Ratio = currency_format(table3['YTD LR'].iloc[-1])
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Nationwide",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP": YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "DWP_12MM": DWP_12MM,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "YTD_PIF": YTD_PIF,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "Loss_Ratio_12MM": Loss_Ratio_12MM,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
            }]
        }
    carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

    # create lob total reports
    # Personal Total Report
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = 'Personal'
    lob_name_string =  Name + ' - ' + Line_of_Business
    Prior_YTD_DWP = currency_format(table1['Prior YTD DWP'].iloc[0])
    YTD_DWP = currency_format(table1['Curr. YTD DWP'].iloc[0])
    YTD_DWP_Growth = currency_format(table1['YTD DWP Growth'].iloc[0])
    DWP_12MM = currency_format(table1['12MM DWP'].iloc[0])
    Prior_YTD_NB_DWP = currency_format(table1['Prior YTD NB DWP'].iloc[0])
    YTD_NB_DWP = currency_format(table1['Curr. YTD NB DWP'].iloc[0])
    YTD_PIF = currency_format(table2['Curr. PIF'].iloc[0])
    YTD_NB_PIF =  currency_format(table2['Curr. YTD NB PIF'].iloc[0])
    Prior_YTD_NB_PIF = currency_format(table2['Prior YTD NB PIF'].iloc[0])
    Prior_YTD_PIF = currency_format(table2['Prior YTD PIF'].iloc[0])
    Loss_Ratio_12MM = currency_format(table3['12MM LR'].iloc[0])
    YTD_Loss_Ratio = currency_format(table3['YTD LR'].iloc[0])
    request_body = {
        "data": [
            {
                "Name": lob_name_string,
                "Carrier": "Nationwide",
                "Report_Type": ['YTD'],
                "Carrier_Report": carrier_report_id,
                "Report_Date": Report_Date,
                'Line_of_Business': Line_of_Business,
                "Prior_YTD_DWP": Prior_YTD_DWP,
                "YTD_DWP": YTD_DWP,
                "YTD_DWP_Growth": YTD_DWP_Growth,
                "DWP_12MM": DWP_12MM,
                "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                "YTD_NB_DWP": YTD_NB_DWP,
                "YTD_PIF": YTD_PIF,
                "YTD_NB_PIF": YTD_NB_PIF,
                "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                "Prior_YTD_PIF": Prior_YTD_PIF,
                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                "YTD_Loss_Ratio": YTD_Loss_Ratio,
            }
        ]
    }
    ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    # personal_id = (ac_lob_total_id["data"][0]["details"]['id'])




    # Commercial Total Report
    Line_of_Business = 'Commercial'
    lob_name_string =  Name + ' - ' + Line_of_Business
    Prior_YTD_DWP = currency_format(table1['Prior YTD DWP'].iloc[1])
    YTD_DWP = currency_format(table1['Curr. YTD DWP'].iloc[1])
    YTD_DWP_Growth = currency_format(table1['YTD DWP Growth'].iloc[1])
    DWP_12MM = currency_format(table1['12MM DWP'].iloc[1])
    Prior_YTD_NB_DWP = currency_format(table1['Prior YTD NB DWP'].iloc[1])
    YTD_NB_DWP = currency_format(table1['Curr. YTD NB DWP'].iloc[1])
    YTD_PIF = currency_format(table2['Curr. PIF'].iloc[1])
    YTD_NB_PIF =  currency_format(table2['Curr. YTD NB PIF'].iloc[1])
    Prior_YTD_NB_PIF = currency_format(table2['Prior YTD NB PIF'].iloc[1])
    Prior_YTD_PIF = currency_format(table2['Prior YTD PIF'].iloc[1])
    Loss_Ratio_12MM = currency_format(table3['12MM LR'].iloc[1])
    YTD_Loss_Ratio = currency_format(table3['YTD LR'].iloc[1])
    request_body = {
        "data": [
            {
                "Name": lob_name_string,
                "Carrier": "Nationwide",
                "Report_Type": ['YTD'],
                "Carrier_Report": carrier_report_id,
                "Report_Date": Report_Date,
                'Line_of_Business': Line_of_Business,
                "Prior_YTD_DWP": Prior_YTD_DWP,
                "YTD_DWP": YTD_DWP,
                "YTD_DWP_Growth": YTD_DWP_Growth,
                "DWP_12MM": DWP_12MM,
                "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                "YTD_NB_DWP": YTD_NB_DWP,
                "YTD_PIF": YTD_PIF,
                "YTD_NB_PIF": YTD_NB_PIF,
                "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                "Prior_YTD_PIF": Prior_YTD_PIF,
                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                "YTD_Loss_Ratio": YTD_Loss_Ratio,
            }
        ]
    }
    ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    # commercial_id = (ac_lob_total_id["data"][0]["details"]['id'])

    # Farm Total Report
    Line_of_Business = 'Farm'
    lob_name_string =  Name + ' - ' + Line_of_Business
    Prior_YTD_DWP = currency_format(table1['Prior YTD DWP'].iloc[2])
    YTD_DWP = currency_format(table1['Curr. YTD DWP'].iloc[2])
    YTD_DWP_Growth = currency_format(table1['YTD DWP Growth'].iloc[2])
    DWP_12MM = currency_format(table1['12MM DWP'].iloc[2])
    Prior_YTD_NB_DWP = currency_format(table1['Prior YTD NB DWP'].iloc[2])
    YTD_NB_DWP = currency_format(table1['Curr. YTD NB DWP'].iloc[2])
    YTD_PIF = currency_format(table2['Curr. PIF'].iloc[2])
    YTD_NB_PIF =  currency_format(table2['Curr. YTD NB PIF'].iloc[2])
    Prior_YTD_NB_PIF = currency_format(table2['Prior YTD NB PIF'].iloc[2])
    Prior_YTD_PIF = currency_format(table2['Prior YTD PIF'].iloc[2])
    Loss_Ratio_12MM = currency_format(table3['12MM LR'].iloc[2])
    YTD_Loss_Ratio = currency_format(table3['YTD LR'].iloc[2])
    request_body = {
        "data": [
            {
                "Name": lob_name_string,
                "Carrier": "Nationwide",
                "Report_Type": ['YTD'],
                "Carrier_Report": carrier_report_id,
                "Report_Date": Report_Date,
                'Line_of_Business': Line_of_Business,
                "Prior_YTD_DWP": Prior_YTD_DWP,
                "YTD_DWP": YTD_DWP,
                "YTD_DWP_Growth": YTD_DWP_Growth,
                "DWP_12MM": DWP_12MM,
                "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                "YTD_NB_DWP": YTD_NB_DWP,
                "YTD_PIF": YTD_PIF,
                "YTD_NB_PIF": YTD_NB_PIF,
                "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                "Prior_YTD_PIF": Prior_YTD_PIF,
                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                "YTD_Loss_Ratio": YTD_Loss_Ratio,
            }
        ]
    }
    ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    # farm_id = (ac_lob_total_id["data"][0]["details"]['id'])

def currency_format(num):
    num = num.replace('$','')
    num = num.replace(',','')
    num = num.replace('%','')
    return num

def format_number(num):
    if(not type(num) == str):
        num = float(num)
        num = num*100

        num_final = round(num, 2)

        if(num_final > 9999):

            num_final = round(num_final, 0)
            num_final = int(num_final)
        if(num_final < -9999):
            num_final = -9999

        if(num_final > 99999):
            num_final = 99999
        if(num_final > 999 and num_final < 9999):
            num_final = round(num_final, 1)
        if(num_final < -99 and num_final > -1000):
            num_final = round(num_final, 1)
        if(num_final < -999):
            num_final = round(num_final, 0)
            num_final = int(num_final)
    else:
        num_final = ''

    return num_final

main()