import requests
import pandas as pd

client_id = '1000.XIFKFVEOT9VGSLZSHUJDYUEJYV2IYY'
client_secret = '5e4298f1f8323e03fbf537c616539062a4209af8da'
refresh_token = '1000.44f0a47a88c00efabf5facc3e6e10ae7.7e8f03a2376d67456d1f41f1c5c868e0'

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

auth_response = requests.post(url=auth_url)

agency_code_search_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes/search'
agency_code_update_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'

access_token = auth_response.json()['access_token']

headers = {
    'Authorization': 'Zoho-oauthtoken ' + access_token
}

def main():
    # replace()
    check_duplicate()

def replace():
    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Travelers',
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
    
    for agency_code in agency_code_records:
        agency_code_name = agency_code['Name']
        if agency_code_name.startswith('O'):
            agency_code_name = "0" + agency_code_name[1:]
            print(agency_code_name)
            print(agency_code['id'])

            agency_code_name = agency_code_name
            update_data = {'data': [{'id': agency_code['id'],
                                     'Name': agency_code_name}]}
            response = requests.put(url=agency_code_update_url,
                                    headers=headers,
                                    json=update_data)
            print(response.json())
    
    return

def check_duplicate():
    agency_code_records = []
    agency_code_names = set()
    page = 1
    more_records = True
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Travelers',
            'page': page
        }
        agency_get_response = requests.get(url=agency_code_search_url,
                                            headers=headers,
                                            params=search_params)
        for i in range(len(agency_get_response.json()['data'])):
            agency_code_records.append(agency_get_response.json()['data'][i])
        if(not agency_get_response.json()['info']['more_records']):
            more_records = False
        page += 1
    
    # Find duplicates and print their names
    for agency_code in agency_code_records:
        agency_code_name = agency_code['Name']
        if agency_code_name in agency_code_names:
            # Duplicate found
            print(f"Duplicate agency code found: {agency_code_name}")
        else:
            agency_code_names.add(agency_code_name)
    


main()