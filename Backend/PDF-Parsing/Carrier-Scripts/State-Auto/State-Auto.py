from unicodedata import name
import pandas as pd
import requests
import json
# import boto3
import io

def main():
    filename = 'State Auto (Production & Loss Summary) - December 2021'

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


    account_id_map = {}
    agency_code_id_map = {}

    #Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    word_date = list_words[6]
    space = list_words[6].index(" ")
    month = list_words[6][:3]
    year = list_words[6][space:]
    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
     # In lambda, modify it to append every table to the df in a loop without manually stating which file
    df = pd.read_csv("December(Production & Loss Summary)//table_e79298fae1704f32abcad520d43fe844_page_1.csv")

    # print(df)
    Name = "State Auto (Production & Loss Summary) - " + word_date
    
    YTD_PIF = currency_format(df[' .2'][6])
    Prior_YTD_PIF = currency_format(df[' .1'][6])
    YTD_NB_PIF = currency_format(df[' .6'][6])
    Prior_YTD_NB_PIF = currency_format(df[' .5'][6])
    YTD_DWP = currency_format(df[' .9'][6])
    Prior_YTD_DWP = currency_format(df['Total'][6])
    YTD_DWP_Growth = currency_format(df[' .10'][6])
    DWP_12MM = currency_format(df[' .12'][6])
    Prior_12MM_DWP = currency_format(df[' .11'][6])
    YTD_NB_DWP = currency_format(df[' .15'][6])
    Prior_YTD_NB_DWP = currency_format(df[' .14'][6])
    YTD_NB_DWP_Growth = currency_format(df[' .16'][6])
    YTD_Loss_Ratio = currency_format(df[' .18'][6])
    
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "State Auto",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "DWP_12MM": DWP_12MM,
                    "Prior_12MM_DWP": Prior_12MM_DWP,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio
            }]
        }
    carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()
    # print(carrier_report_id)
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

     # create Personal LOB report
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = 'Personal'
    name_string = Name + ' - ' + Line_of_Business
    YTD_PIF = currency_format(df[' .2'][2])
    Prior_YTD_PIF = currency_format(df[' .1'][2])
    YTD_NB_PIF = currency_format(df[' .6'][2])
    Prior_YTD_NB_PIF = currency_format(df[' .5'][2])
    YTD_DWP = currency_format(df[' .9'][2])
    Prior_YTD_DWP = currency_format(df['Total'][2])
    DWP_12MM = currency_format(df[' .12'][2])
    Prior_12MM_DWP = currency_format(df[' .11'][2])
    YTD_NB_DWP = currency_format(df[' .15'][2])
    Prior_YTD_NB_DWP = currency_format(df[' .14'][2])
    YTD_NB_DWP_Growth = currency_format(df[' .16'][2])
    YTD_Loss_Ratio = currency_format(df[' .18'][2])
    
    request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier": "State Auto",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Carrier_Report": carrier_report_id,
                    "Line_of_Business": Line_of_Business,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "DWP_12MM": DWP_12MM,
                    "Prior_12MM_DWP": Prior_12MM_DWP,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio
            }]
        }
    ac_lob = requests.post(
    url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())

    # create Commercial LOB report
    Line_of_Business = 'Commercial'
    name_string = Name + ' - ' + Line_of_Business
    YTD_PIF = currency_format(df[' .2'][3])
    Prior_YTD_PIF = currency_format(df[' .1'][3])
    YTD_NB_PIF = currency_format(df[' .6'][3])
    Prior_YTD_NB_PIF = currency_format(df[' .5'][3])
    YTD_DWP = currency_format(df[' .9'][3])
    Prior_YTD_DWP = currency_format(df['Total'][3])
    DWP_12MM = currency_format(df[' .12'][3])
    Prior_12MM_DWP = currency_format(df[' .11'][3])
    YTD_NB_DWP = currency_format(df[' .15'][3])
    Prior_YTD_NB_DWP = currency_format(df[' .14'][3])
    YTD_NB_DWP_Growth = currency_format(df[' .16'][3])
    YTD_Loss_Ratio = currency_format(df[' .18'][3])
    
    request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier": "State Auto",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Carrier_Report": carrier_report_id,
                    "Line_of_Business": Line_of_Business,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "DWP_12MM": DWP_12MM,
                    "Prior_12MM_DWP": Prior_12MM_DWP,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio
            }]
        }
    ac_lob = requests.post(
    url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())

    # create Farm & Ranch LOB report
    Line_of_Business = 'Farm & Ranch'
    name_string = Name + ' - ' + Line_of_Business
    YTD_PIF = currency_format(df[' .2'][4])
    Prior_YTD_PIF = currency_format(df[' .1'][4])
    YTD_NB_PIF = currency_format(df[' .6'][4])
    Prior_YTD_NB_PIF = currency_format(df[' .5'][4])
    YTD_DWP = currency_format(df[' .9'][4])
    Prior_YTD_DWP = currency_format(df['Total'][4])
    DWP_12MM = currency_format(df[' .12'][4])
    Prior_12MM_DWP = currency_format(df[' .11'][4])
    YTD_NB_DWP = currency_format(df[' .15'][4])
    Prior_YTD_NB_DWP = currency_format(df[' .14'][4])
    YTD_NB_DWP_Growth = currency_format(df[' .16'][4])
    YTD_Loss_Ratio = currency_format(df[' .18'][4])
    
    request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier": "State Auto",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Carrier_Report": carrier_report_id,
                    "Line_of_Business": Line_of_Business,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "DWP_12MM": DWP_12MM,
                    "Prior_12MM_DWP": Prior_12MM_DWP,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio
            }]
        }
    ac_lob = requests.post(
    url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())

def currency_format(num):
    num = num.replace('$','')
    num = num.replace(',','')
    num = num.replace('%','')
    num = num.replace('(','')
    num = num.replace(')','')
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