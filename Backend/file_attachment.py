from calendar import c
from itertools import count
import requests
import pandas as pd
import json
# import boto3

client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'

# this is the authorization url. As you can see it is just an endpoint (URL) that uses the above variables to retrieve an access token.
# Fundamentally, what an API call does in any language (python, javascript), is put the info in the correct place in the URL, access it, and return a response
# based on what has been programmed on the other side of the API to respond.

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

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

carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'

def main():
    # This is the search function to find all existing carrier reports
    coql_url = 'https://www.zohoapis.com/crm/v2/coql'
    more_records = True
    query_offset = 0
    reports = []
    # s3 = boto3.resource('s3')

    # User.json from s3
    # obj = s3.Object('textractkvtable-us-east-1-907387566050', 'users.json')
    # data = {"carrier":"State Auto","filename":"State Auto+(Commercial)+-+March+2022.xlsx","key":"1488385.xlsx"}
    data = {"carrier":"Kemper","filename":"Kemper -+July+2022.xlsx","key":"1551784.xlsx"}
    # data = {"carrier":"Travelers","filename":"Travelers -+Production+Data+Summary+-+March+2022.pdf","key":"9422653.pdf"}
    # data = json.load(obj.get()['Body'])
    carrier = data["carrier"]
    carrier = "'" + carrier + "'"

    # Filename
    original_filename = data['filename']
    filename = data["filename"]
    filename = filename.replace("+",' ')
    dot = filename.index(".")
    filename = filename[:dot]
    # print(filename)

    # Key word and month_year
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]
    print(list_words)
    month_year = list_words[-1]
    key_word = list_words[2]
    print(key_word)

    # # Grab the file from textract s3 bucket (PDF) or excel bucket
    # key = data['key']
    # if '.pdf' in original_filename:
    #     file = s3.Object('textractkvtable-us-east-1-907387566050', key)
    # elif '.xlsx' in original_filename:
    #     file = s3.Object('excel-parser-files', key)


    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100
    # print(reports)

    for report in reports:
        for i in report:
            report_name = i["Name"]
            if carrier == "'Travelers'":
                if key_word in report_name:
                    if month_year in report_name:
                        id = i['id']
                        print(report_name)
            elif carrier == "'State Auto'":
                if key_word in report_name:
                    if month_year in report_name:
                        id = i['id']
                        print(report_name)
            elif carrier == "'Nationwide'":
                if key_word in report_name:
                    if month_year in report_name:
                        id = i['id']
                        print(report_name)
            elif carrier == "'Safeco'":
                if key_word in report_name:
                    if month_year in report_name:
                        id = i['id']
                        print(report_name)
            else:
                if month_year in report_name:
                    id = i['id']
                    print(report_name)
                    print(id)

    # Upload file to Carrier Report with Carrier Report ID
    

main()