from cmath import nan
from unicodedata import name
from numpy import NaN
import pandas as pd
import requests
import json

consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'

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
carrier_data = pd.read_excel("Grange Agency Collective 01 2022.xlsx")


# Change fields based on report
def main():
    carrier_data = pd.read_excel("Grange Agency Collective 01 2022.xlsx")
    carrier_data = carrier_data.fillna(0)
    carrier_data.columns = carrier_data.iloc[2]
    cols=pd.Series(carrier_data.columns)
    for dup in carrier_data.columns[carrier_data.columns.duplicated(keep=False)]: 
            cols[carrier_data.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                            if d_idx != 0 
                                            else dup 
                                            for d_idx in range(carrier_data.columns.get_loc(dup).sum())]
                                            )
    carrier_data.columns=cols
    print(carrier_data)
    for i, row in carrier_data.iterrows():
        if i == 5:
            print(row)
    # Copy starting here for carrier report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Report_Date = (carrier_data["Agency Number"][1])
    Report_Date = Report_Date.replace(',','')
    Name = "Grange - " + Report_Date + "TESTING"
    space = Report_Date.index(" ")
    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=29,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    year = Report_Date[space+1:]
    month = Report_Date[:3]

    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    YTD_DWP = int(carrier_data["YTD DWP"][3]) + int(carrier_data["YTD DWP.1"][3]) + int(carrier_data["YTD DWP.2"][3])
    print(YTD_DWP)
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"][3]) + int(carrier_data["Prior YTD DWP.1"][3]) + int(carrier_data["Prior YTD DWP.2"][3])
    DWP_12MM =  int(carrier_data["Rolling 12 DWP"][3]) + int(carrier_data["Rolling 12 DWP.1"][3]) + int(carrier_data["Rolling 12 DWP.2"][3])
    Month_DWP = int(carrier_data["Month DWP"][3]) + int(carrier_data["Month DWP.1"][3]) + int(carrier_data["Month DWP.2"][3])
    YTD_NB_DWP =  int(carrier_data["YTD NB"][3]) + int(carrier_data["YTD NB.1"][3]) + int(carrier_data["YTD NB.2"][3])
    print(YTD_NB_DWP)
    Prior_YTD_NB_DWP = int(carrier_data["Prior YTD NB"][3]) + int(carrier_data["Prior YTD NB.1"][3]) + int(carrier_data["Prior YTD NB.2"][3])
    print(Prior_YTD_NB_DWP)
    NB_DWP_12MM = int(carrier_data["Rolling 12 NB"][3]) + int(carrier_data["Rolling 12 NB.1"][3]) + int(carrier_data["Rolling 12 NB.2"][3])
    YTD_PIF = int(carrier_data["YTD PIF"][3]) + int(carrier_data["YTD PIF.1"][3]) + int(carrier_data["YTD PIF.2"][3])
    Prior_YTD_PIF = int(carrier_data["Prior YTD PIF"][3]) + int(carrier_data["Prior YTD PIF.1"][3]) + int(carrier_data["Prior YTD PIF.2"][3])
    YTD_Quotes = int(carrier_data["YTD Quotes"][3]) + int(carrier_data["YTD Quotes.1"][3]) + int(carrier_data["YTD Quotes.2"][3])
    Quotes_12MM = int(carrier_data["Rolling 12 Quotes"][3]) + int(carrier_data["Rolling 12 Quotes.1"][3]) + int(carrier_data["Rolling 12 Quotes.2"][3])
    Prior_YTD_Quotes = int(carrier_data["Quote Count - Prior YTD"][3]) + int(carrier_data["Prior YTD Quotes"][3]) + int(carrier_data["Prior YTD Quotes.1"][3])
    req_body = {
                    "data": [
                        {
                            'Carrier': "Grange",
                            'Name': Name,
                            'Report_Type': ['Monthly','YTD'],
                            'Report_Date': Report_Date,
                            'YTD_DWP': YTD_DWP,
                            'Prior_YTD_DWP': Prior_YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            'Month_DWP': Month_DWP,
                            'YTD_NB_DWP': YTD_NB_DWP,
                            'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'YTD_PIF': YTD_PIF,
                            'Prior_YTD_PIF': Prior_YTD_PIF,
                            'YTD_Quotes': YTD_Quotes,
                            'Prior_YTD_Quotes': Prior_YTD_Quotes,
                            'Quotes_12MM': Quotes_12MM
                        }
                    ]
                }
    carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
    carrier_report_id = (carrier_report.json())
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])


    # Copy Starting Here for Agency LOB Total Reports
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Personal'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP"][3])
    Month_DWP = int(carrier_data["Month DWP"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB'][3])
    YTD_PIF = (carrier_data['YTD PIF'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    pl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])

    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Commercial'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP.1"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP.1"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP.1"][3])
    Month_DWP = int(carrier_data["Month DWP.1"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB.1'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB.1'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB.1'][3])
    YTD_PIF = (carrier_data['YTD PIF.1'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF.1'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR.1'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR.1'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes.1'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes.1'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    cl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])

    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Specialty'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP.2"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP.2"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP.2"][3])
    Month_DWP = int(carrier_data["Month DWP.2"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB.2'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB.2'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB.2'][3])
    YTD_PIF = (carrier_data['YTD PIF.2'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF.2'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR.2'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR.2'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes.2'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes.1'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes.2'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    sl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])

def format_percentage(num):

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
    return num_final

main()