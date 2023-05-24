from unicodedata import name
import pandas as pd
import requests
import json
# import boto3
import io

def main():
    filename = 'Liberty Mutual(YE Results) - December 2021'

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

    word_date = list_words[4]
    space = list_words[4].index(" ")
    month = list_words[4][:3]
    year = list_words[4][space:]
    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
     # In lambda, modify it to append every table to the df in a loop without manually stating which file
    table1 = pd.read_csv("table_624a539781864fba810f1fc29673b36d_page_1.csv")
    table2 = pd.read_csv("table_96d888d0b51646e6810a20239a68a2f6_page_1.csv")
    table3 = pd.read_csv("table_3e78dd08f35b401294f21d273a6f8ffe_page_1.csv")
    table4 = pd.read_csv("table_af550b042e994dad96f89eab7a1fd7cb_page_1.csv")
    table5 = pd.read_csv("table_447e3de932ce414597c260e4ecc0465b_page_2.csv")
    table6 = pd.read_csv("table_03f6a4482fa346abaa425225c27ebef5_page_2.csv")
    table7 = pd.read_csv("table_6ae828a93d5342dda4f89cea093041d1_page_2.csv")
    table8 = pd.read_csv("table_906a1263ccd449ad8091e329c201c414_page_2.csv")
    table9 = pd.read_csv("table_d250d3d0bdb845e6aaba700f6ef0f272_page_3.csv")

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(table2)
    # print(table2.columns)

    # print(df)
    Name = "Liberty Mutual(YE Results) - " + word_date
    
    YTD_PIF = currency_format(table1['Policy Lines'].iloc[-1])
    YTD_PIF_Growth = currency_format(table1['Inforce (PLIF)'].iloc[-1])
    YTD_DWP = currency_format(table1[' .2'].iloc[-1])
    Prior_YTD_DWP = currency_format(table1['Direct Written'].iloc[-1])
    YTD_DWP_Growth = currency_format(table1['Premium (DWP)'].iloc[-1])
    YTD_NB_DWP = currency_format(table1['New'].iloc[-1])
    YTD_NB_DWP_Growth = currency_format(table1['Business DWP'].iloc[-1])
    YTD_Loss_Ratio = currency_format(table2['Profitability'].iloc[-1])
    Earned_Premium_YTD = currency_format(table2[' .2'].iloc[-1])
    Incurred_Loss_YTD = currency_format(table2[' .3'].iloc[-1])
    
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Liberty Mutual",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_PIF": YTD_PIF,
                    "YTD_PIF_Growth": YTD_PIF_Growth,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "YTD_NB_DWP_Growth":YTD_NB_DWP_Growth,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "Incurred_Loss_YTD": Incurred_Loss_YTD
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
    length = len(table5[' .1']) - 2
    for i, row in table5.iterrows():
        if i < length and i != 0:
            print(i)
            Line_of_Business = row[" .1"]
            name_string = Name + ' - ' + Line_of_Business
            YTD_PIF = currency_format(row['Policy Lines'])
            YTD_PIF_Growth = currency_format(row['Inforce (PLIF)'])
            YTD_DWP = currency_format(row[' .2'])
            Prior_YTD_DWP = currency_format(row['Direct Written'])
            YTD_DWP_Growth = currency_format(row['Premium (DWP)'])
            YTD_NB_DWP = currency_format(row['New'])
            YTD_NB_DWP_Growth = currency_format(row['Business DWP'])
            YTD_Loss_Ratio = currency_format(table6['Profitability'].iloc[i])
            Earned_Premium_YTD = currency_format(table6[' .2'].iloc[i])
            Incurred_Loss_YTD = currency_format(table6[' .3'].iloc[i])
            request_body = {
                    "data": [{
                            "Name": name_string,
                            "Carrier": "Liberty Mutual",
                            "Report_Type": ['YTD'],
                            "Report_Date": Report_Date,
                            "Carrier_Report": carrier_report_id,
                            "Line_of_Business": Line_of_Business,
                            "YTD_PIF": YTD_PIF,
                            "YTD_PIF_Growth": YTD_PIF_Growth,
                            "YTD_DWP": YTD_DWP,
                            "Prior_YTD_DWP": Prior_YTD_DWP,
                            "YTD_DWP_Growth": YTD_DWP_Growth,
                            "YTD_NB_DWP": YTD_NB_DWP,
                            "YTD_NB_DWP_Growth":YTD_NB_DWP_Growth,
                            "YTD_Loss_Ratio": YTD_Loss_Ratio,
                            "Earned_Premium_YTD": Earned_Premium_YTD,
                            "Incurred_Loss_YTD": Incurred_Loss_YTD
                    }]
                }
            ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
            ac_lob_total_id = (ac_lob.json())
            # print(ac_lob_total_id)

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