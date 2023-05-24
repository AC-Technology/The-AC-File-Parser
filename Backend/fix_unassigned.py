import pandas as pd
import requests
import json

# Global Variables
client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'
coql_url = 'https://www.zohoapis.com/crm/v2/coql'

carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

auth_response = requests.post(url=auth_url)

access_token = auth_response.json()['access_token']

headers = {
    'Authorization': 'Zoho-oauthtoken ' + access_token
}

# This script assigns an unassigned account. Make sure to change the carrier in the agency codes function.
def main():
    agency_reports = unassigned_agency_reports()
    agency_lob_reports = unassigned_agency_lob_reports()
    agency_code_id_map = agency_codes()
    count = 0

    # loop through each report in agency reports
    for report in agency_reports:
        # loop through every record in each report
        for i in report:
            agency_report_id = (i['id'])
            agency_code_id =(i['Agency_Code']['id'])
            # find the account id by matching agency codes
            try:
                account_id = agency_code_id_map[agency_code_id]["acc_id"]
                # Update the agency report's account
                request_body = {
                    "data": [{
                                "id": agency_report_id,
                                "Account" : account_id
                            }]
                }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
            except:
                count += 1
    print("Number of agency reports still unassigned:",count)

    count = 0

    # loop through each report in agency lob reports
    for report in agency_lob_reports:
        # loop through every record in each report
        for i in report:
            agency_lob_report_id = (i['id'])
            agency_code_id =(i['Agency_Code']['id'])
            # find the account id by matching agency codes
            try:
                account_id = agency_code_id_map[agency_code_id]["acc_id"]
                # Update the agency lob report's account
                request_body = {
                    "data": [{
                                "id": agency_lob_report_id,
                                "Account" : account_id
                            }]
                }
                member_lob_response = requests.put(
                            url=member_lob_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                member_lob_response = member_lob_response.json()
            except:
                count += 1
    print("Number of agency reports still unassigned:",count)

    return
    
# This is the modified agency code search function where it assigned the agency code id as the key and the account id as the value. {key : value}
# Make sure to change the carrier in the search parameters
def agency_codes():
    more_records = True
    agency_code_records =[]
    agency_code_search_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes/search'
    agency_code_id_map = {}

    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Safeco',
            'page':page
        }
        agency_get_response = requests.get(url=agency_code_search_url,
                                        headers=headers,
                                        params=search_params)
        for i in range(len(agency_get_response.json()['data'])):
            agency_code_records.append(agency_get_response.json()['data'][i])
        if(not agency_get_response.json()['info']['more_records']):
            more_records = False
        page +=1

    for i in range(len(agency_code_records)):
        current_record = agency_code_records[i]
        try:
            agency_code_id_map[current_record['id']] = {
                    'acc_id': current_record['Account']['id']
                }
        except:
            print("There is a agency code without an account/agency linked to it. This means if you look at the agency code the account/agency is empty.")
            pass

    return agency_code_id_map

# This function grabs all the agency reports with an unassigned account
def unassigned_agency_reports():
    more_records = True
    query_offset = 0
    reports = []
    has_Report = False

    # Grab all the agency reports with the account as unassigned
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Agency_Code from Member_Data_Consolidated where Account = 5187612000000754742 limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member Reports or there is an error")
            more_records = False
    
    return reports

# This function grabs all the agency LOB reports with an unassigned account 
def unassigned_agency_lob_reports():
    more_records = True
    query_offset = 0
    reports = []
    has_Report = False


    # Grab all the agency reports with the account as unassigned
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Agency_Code from Member_LOB_Data where Account = 5187612000000754742 limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Agency LOB Reports or there is an error")
            more_records = False
    
    return reports

main()