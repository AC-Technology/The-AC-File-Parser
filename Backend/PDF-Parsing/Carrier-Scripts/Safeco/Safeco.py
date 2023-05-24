from unicodedata import name
import pandas as pd
import requests
import json
# import boto3
import io

def main():
    filename = 'Safeco(YE Results) - December 2021'

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

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
     # In lambda, modify it to append every table to the df in a loop without manually stating which file
    table1 = pd.read_csv("table_be8422c6cc6e4e05a4a37f213d5d35ea_page_1.csv")
    table2 = pd.read_csv("table_58ced1d95bf746ef81dfa10455f68c68_page_1.csv")
    table3 = pd.read_csv("table_b6724d195ff2409e9630bb5ddfc68919_page_2.csv")
    table4 = pd.read_csv("table_0e85f3341f01448f97cb3a65fdfbc165_page_2.csv")


    table1.columns = table1.iloc[0]
    table1 = table1.iloc[1:]
    cols=pd.Series(table1.columns)
    for dup in table1.columns[table1.columns.duplicated(keep=False)]: 
        cols[table1.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(table1.columns.get_loc(dup).sum())]
                                        )
    table1.columns=cols
    table1 = table1.reset_index()
    print(table1)
    print("")

    table2.columns = table2.iloc[0]
    table2 = table2.iloc[1:]
    cols=pd.Series(table2.columns)
    for dup in table2.columns[table2.columns.duplicated(keep=False)]: 
        cols[table2.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(table2.columns.get_loc(dup).sum())]
                                        )
    table2.columns=cols
    table2 = table2.reset_index()

    Name = "Safeco(YE Results) - " + word_date
    
    YTD_DWP = currency_format(table1["YTD"].iloc[-1])
    DWP_12MM = currency_format(table1["Rolling 12"].iloc[-1])
    Prior_YTD_DWP = currency_format(table3["Direct Written"].iloc[-1])
    YTD_NB_DWP =  currency_format(table1["YTD"].iloc[-1])
    Prior_YTD_NB_DWP = currency_format(table4["Written Premium"].iloc[-1])
    YTD_DWP_Growth = currency_format(table1["YTD Growth Rate"].iloc[-1])
    Earned_Premium_12MM = currency_format(table1["R12 EP"].iloc[-1])
    Earned_Premium_YTD = currency_format(table1["YTD EP"].iloc[-1])
    Incurred_Loss_YTD = currency_format(table1["YTD Inc Losses"].iloc[-1])
    YTD_Loss_Ratio = currency_format(table1["YTD Loss Ratio"].iloc[-1])
    YTD_Quotes = currency_format(table2["YTD.2"].iloc[-1])
    Prior_YTD_Quotes = currency_format(table4["Quotes"].iloc[-1])
    YTD_Hit_Ratio = currency_format(table2["YTD Close Ratio"].iloc[-1])
    YTD_PIF = currency_format(table1["Current PIF Count"].iloc[-1])
    YTD_PIF_Growth = currency_format(table1["YTD Growth Rate.1"].iloc[-1])
    Prior_YTD_PIF = currency_format(table3["Inforce (PIF)"].iloc[-1])
    
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Safeco",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_DWP": YTD_DWP,
                    "DWP_12MM": DWP_12MM,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "Earned_Premium_12MM": Earned_Premium_12MM,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "Incurred_Loss_YTD": Incurred_Loss_YTD,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "YTD_Quotes": YTD_Quotes,
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    "YTD_Hit_Ratio": YTD_Hit_Ratio,
                    "YTD_PIF": YTD_PIF,
                    "YTD_PIF_Growth": YTD_PIF_Growth,
                    "Prior_YTD_PIF": Prior_YTD_PIF
            }]
        }
    # print(request_body)
    carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()
    # print(carrier_report_id)
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

    # create LOB reports
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    for i, row in table1.iterrows():
            if row['Product'] != ' ':
                Line_of_Business = row["Product"]
                name_string = Name + ' - ' + Line_of_Business
                YTD_DWP = currency_format(row["YTD"])
                DWP_12MM = currency_format(row["Rolling 12"])
                Prior_YTD_DWP = currency_format(table3["Direct Written"].iloc[i+1])
                YTD_NB_DWP =  currency_format(row["YTD"])
                Prior_YTD_NB_DWP = currency_format(table4["Written Premium"].iloc[i+1])
                YTD_DWP_Growth = currency_format(row["YTD Growth Rate"])
                Earned_Premium_12MM = currency_format(row["R12 EP"])
                Earned_Premium_YTD = currency_format(row["YTD EP"])
                Incurred_Loss_YTD = currency_format(row["YTD Inc Losses"])
                YTD_Loss_Ratio = currency_format(row["YTD Loss Ratio"])
                YTD_Quotes = currency_format(table2["YTD.2"].iloc[i+1])
                Prior_YTD_Quotes = currency_format(table4["Quotes"].iloc[i+1])
                YTD_Hit_Ratio = currency_format(table2["YTD Close Ratio"].iloc[i+1])
                YTD_PIF = currency_format(row["Current PIF Count"])
                YTD_PIF_Growth = currency_format(row["YTD Growth Rate.1"])
                Prior_YTD_PIF = currency_format(table3["Inforce (PIF)"].iloc[i+1])
                request_body = {
                    "data": [{
                        "Name": name_string,
                        "Carrier": "Safeco",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "Carrier_Report": carrier_report_id,
                        "Line_of_Business": Line_of_Business,
                        "YTD_DWP": YTD_DWP,
                        "DWP_12MM": DWP_12MM,
                        "Prior_YTD_DWP": Prior_YTD_DWP,
                        "YTD_DWP_Growth": YTD_DWP_Growth,
                        "YTD_NB_DWP": YTD_NB_DWP,
                        "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                        "Earned_Premium_12MM": Earned_Premium_12MM,
                        "Earned_Premium_YTD": Earned_Premium_YTD,
                        "Incurred_Loss_YTD": Incurred_Loss_YTD,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "YTD_Hit_Ratio": YTD_Hit_Ratio,
                        "YTD_PIF": YTD_PIF,
                        "YTD_PIF_Growth": YTD_PIF_Growth,
                        "Prior_YTD_PIF": Prior_YTD_PIF
                    }]
                }
                # print(request_body)
                ac_lob = requests.post(
                url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                ac_lob_total_id = (ac_lob.json())
                # print(ac_lob_total_id)

def currency_format(num):
    if(num != str):
        num = str(num)
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