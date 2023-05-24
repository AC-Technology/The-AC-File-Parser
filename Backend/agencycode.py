import pandas as pd
import requests
import json
import io

def main():
    count = 0
    
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

    # read carrier data in using pandas


    account_id_map = {}
    agency_code_id_map = {}

    # This is the search function to find all existing account ids and adds them to a account id map
    coql_url = 'https://www.zohoapis.com/crm/v2/coql'
    more_records = True
    query_offset = 0
    agency_id_records = []
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Account_Name, Primary_Contact from Accounts where Account_Name != '' limit " + str(query_offset) + ", 100"

                                        }))
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        agency_id_records.append(coql_response.json()['data'])
        query_offset += 100


    # loop through each list in the hash map 
    count = 0
    for i in range(len(agency_id_records)):
        for x in range(len(agency_id_records[i])):
            current_record = (agency_id_records[i][x])
            count += 1
            account_name = current_record['Account_Name'].strip(
            ).lower().replace(',', "").replace('.', "")

            account_id_map[account_name] = {
                'acc_id': str(current_record['id']),
              }
        try:
            [account_name]['cont_id'] = str(current_record['Primary_Contact']['id'])

        except TypeError:
                account_id_map[account_name]['cont_id'] = ''

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    lob_member_data = []


    # This is the search function used to find all existing agency codes and adds them to the agency code map
    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Main Street America',
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
            test = current_record['Account']['id']
            agency_code_id_map[current_record['Name']] = {
                'agency_code_id': current_record['id'],
                    'acc_id': current_record['Account']['id'],
                }
            try:
                agency_code_id_map[current_record['Name']
                                    ]['contact_id'] = current_record['Contact']['id']
            except:
                agency_code_id_map[current_record['Name']]['contact_id'] = ''
        except:
            pass
main()