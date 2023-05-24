from calendar import c
from itertools import count
import requests
import pandas as pd
import json

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
    # This is the search function to find all existing agency codes
    coql_url = 'https://www.zohoapis.com/crm/v2/coql'
    reports = []
    query_offset = 0
    more_records = True
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Name, UAC_Name, Account from Agency_Codes where Account = '5187612000000560802' limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100
    print(reports)


    count = 0
    for i in reports:
        for j in i:
            count += 1
            UAC_Name = UAC_Name = "'" + j["UAC_Name"] + "'"
            agency_code = j['Name']
            current_id = j['id']
            # print(UAC_Name)
            # print(agency_code)
            if UAC_Name != "Insurance Alliance":
                agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'

                account_id = 5187612000000754742

                request_body = {
                            "data": [{
                                    "id": current_id,
                                    "Account": account_id
                            }]
                        }

                agency_code = requests.put(
                            url=agency_code_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                agency_code = agency_code.json()
                print(count)
                print(agency_code)


main()