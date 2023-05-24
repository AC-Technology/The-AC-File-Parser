from cmath import nan
from email import header
from numpy import NaN, rec
import pandas as pd
from difflib import SequenceMatcher
import requests
import json


# GLOBAL VARIABLES

# these are the client id, secret, and the refresh token you will be using to make API calls to the CRM API
# they are given to us by zoho and are unique to every CRM

client_id = '1000.XIFKFVEOT9VGSLZSHUJDYUEJYV2IYY'
client_secret = '5e4298f1f8323e03fbf537c616539062a4209af8da'
refresh_token = '1000.44f0a47a88c00efabf5facc3e6e10ae7.7e8f03a2376d67456d1f41f1c5c868e0'

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


def main():

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

    headers = {'Authorization': 'Zoho-oauthtoken ' + access_token}

    # read carrier and zoho data in using pandas
    carrier_data = pd.read_excel('Liberty-Mutual\Carrier-Data\Liberty Mutual Agency Collective Jan 2022.xlsx', 'Sub Agency')
    zoho_account_data = pd.read_csv('Zoho-Data\Accounts_001.csv')

    # create id dictionaries (maps)
    account_id_map = {}
    agency_code_id_map = {}

  
    # this is our zoho account data map, it will help us match agency codes we will have to create

    for i, row in zoho_account_data.iterrows():
        # this horrific piece of string manipulation removes trailing space, puts everything in lowercase, and removes all periods and commas.
        account_name = str(row['Account Name']).strip().lower().replace(
            ',', "").replace('.', "")

        account_id_map[account_name] = {
            'acc_id': str(row['Record Id']).split('_')[1],
        }
        try:
            account_id_map[account_name]['cont_id'] = str(row['Primary Contact ID']).split('_')[1]
        except IndexError:
            account_id_map[account_name]['cont_id'] = ''

    # this is the carrier report id of the AC Carrier Report record you created for this carrier report. If you haven't made one, you must
    # make one in order for this to work properly

    carrier_report_id = 5187612000002197589

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    lob_member_data = []
    carrier_data.columns = carrier_data.loc[14]

    account_id = ''
    contact_id = ''
    agency_code_id = ''
    uac_number = ''  
    agency_code = ''
    member_consolidated_id = ''

    count = 0
    for i, row in carrier_data[15:266].iterrows():         
        count += 1
        print(count)

        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        agency_name = str(row['Agency Name']).strip()

        agency_code = str(row["Sub Agency Code"]).strip()

        name_string = agency_name + " - Liberty Mutual - Jan 2022"

        DWP_12MM = row["CY R12 DWP"]

        YTD_DWP = row["CY YTD DWP"]

        Prior_YTD_DWP = row['PY YTD DWP']

        YTD_DWP_Growth = format_number(row["YTD DWP Growth"])

        YTD_NB_DWP = row["CY YTD NB"]

        Prior_YTD_NB_DWP = row["PY YTD NB"]

        YTD_NB_DWP_Growth = format_number(row["YTD NB Growth %"])

        NB_DWP_12MM = row["CY R12 NB"]

        YTD_Quotes = row["CY YTD Quotes"]

        Prior_YTD_Quotes = row["PY YTD Quotes"]

        YTD_Quote_Growth = format_number(row["YTD Quote Growth %"])

        YTD_Hit_Ratio = format_number(row["CY Hit Ratio %"])

        Prior_YTD_Hit_Ratio = format_number(row["PY Hit Ratio %"])

        YTD_PIF = row["PLIF YTD"]

        Earned_Premium_YTD = row["CY YTD EP"]
         	
        YTD_Loss_Ratio = format_number(row["CAL YTD Loss Ratio %"])

        YTD_Quote_Growth = format_number(row["YTD Quote Growth %"])


        # print(i)
        if i != 15:
            # if agency code is a duplicate
            try:
                search_params = {
                    'criteria': 'Name:equals:' + str(agency_code),
                    'carrier' : 'Liberty Mutual'
                }
                agency_get_response = requests.get(
                    url=agency_code_search_url, headers=headers, params=search_params)
                agency_code_record = agency_get_response.json()['data'][0]



            # If the agency is not a duplicate create a new code
            except requests.exceptions.JSONDecodeError :
                # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
                account_id = ''
                contact_id = ''
                agency_code_id = ''
                uac_number = ''  
                member_consolidated_id = ''
                try:
                    # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
                    account_id = agency_code_id_map[agency_code]["acc_id"]
                    agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
                    contact_id = agency_code_id_map[agency_code]["contact_id"]
                    # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.
                    # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names
                except:
                    # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

                    formatted_agency_name = agency_name.strip(
                    ).lower().replace(',', "").replace('.', "")

                    try:
                        account_id = account_id_map[formatted_agency_name]['acc_id']
                        contact_id = account_id_map[formatted_agency_name]['cont_id']
                    except KeyError:
                        pass

                    if(account_id == ''):
                        # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.
                        account_id = 5187612000000754742

                    # we found an account id match so now we just need to create our agency code record
                    request_body = {
                        "data": [
                            {
                                "Account": account_id,
                                "Name": agency_code,
                                "UAC": uac_number,
                                "Carrier": "Liberty Mutual",
                                "UAC_Name": agency_name,
                                "Contact": contact_id
                                # add additional fields here
                            }
                        ]
                    }

                    # this is our API call to the CRM API to post a record in the Agency Code Module.
                    agency_code_response = requests.post(
                        url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))


                    if(agency_code_response.json()['data'][0]['status'] == "error"):
                        print(agency_code_response.request)
                        print(request_body)
                        print(agency_code_response.json())

                        print("  ")
                    
                        print("  ")
                        return
                    
                    
                    # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

                    agency_code_id = agency_code_response.json()[
                        'data'][0]['details']['id']
                    new_agency_code_count += 1

                # Create Member Consolidated   
                request_body = {
                    "data": [{
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        "Carrier": "Liberty Mutual",
                        "Report_Date": "2022-01-31",
                        'Report_Type': ['YTD'],
                        "DWP_12MM": DWP_12MM,
                        "YTD_DWP": YTD_DWP,
                        "Prior_YTD_DWP": Prior_YTD_DWP,
                        "YTD_DWP_Growth": YTD_DWP_Growth,
                        "YTD_NB_DWP": YTD_NB_DWP,
                        "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                        "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                        "NB_DWP_12MM": NB_DWP_12MM,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "YTD_Hit_Ratio": YTD_Hit_Ratio,
                        "Prior_YTD_Hit_Ratio": Prior_YTD_Hit_Ratio,
                        "Earned_Premium_YTD": Earned_Premium_YTD,
                        "YTD_PIF": YTD_PIF,
                        "YTD_Loss_Ratio" : YTD_Loss_Ratio,
                        "YTD_Quote_Growth": YTD_Quote_Growth

                        # add additional fields here
                    }]
                }

                # now we make our member consolidated record.

                # this is our API call to the CRM API to post a record in the Member Consolidated Module.
                member_consolidated_count += 1
                consolidated_response = requests.post(
                    url=consolidated_url,
                    headers=headers,
                    data=json.dumps(request_body).encode('utf-8'))

                if (consolidated_response.json()['data'][0]['status'] == "error"):
                    print("  ")
                    print(request_body)
                    print(consolidated_response.json())
                    print("  ")
                    return


        # This is for the first iteration
        if i == 15:
            try:
                # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
                account_id = agency_code_id_map[agency_code]["acc_id"]
                agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
                contact_id = agency_code_id_map[agency_code]["contact_id"]
                # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.
                # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names
            except:
                # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

                formatted_agency_name = agency_name.strip(
                ).lower().replace(',', "").replace('.', "")

                try:
                    account_id = account_id_map[formatted_agency_name]['acc_id']
                    contact_id = account_id_map[formatted_agency_name]['cont_id']
                except KeyError:
                    pass

                if(account_id == ''):
                    # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.
                    account_id = 5187612000000754742

                # we found an account id match so now we just need to create our agency code record
                request_body = {
                    "data": [
                        {
                            "Account": account_id,
                            "Name": agency_code,
                            "UAC": uac_number,
                            "Carrier": "Liberty Mutual",
                            "UAC_Name": agency_name,
                            "Contact": contact_id
                            # add additional fields here
                        }
                    ]
                }

                # this is our API call to the CRM API to post a record in the Agency Code Module.
                agency_code_response = requests.post(
                    url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

                if(agency_code_response.json()['data'][0]['status'] == "error"):
                    print(agency_code_response.request)
                    print(request_body)
                    print(agency_code_response.json())

                    print("  ")
                
                    print("  ")
                    return
                
                
                # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

                agency_code_id = agency_code_response.json()[
                    'data'][0]['details']['id']
                new_agency_code_count += 1

                

            # Create Member Consolidated    
            request_body = {
                "data": [{
                    "Name": name_string,
                    "Carrier_Report": carrier_report_id,
                    "Account": account_id,
                    "Agency_Code": agency_code_id,
                    "Contact": contact_id,
                    "Carrier": "Liberty Mutual",
                    "Report_Date": "2022-01-31",
                    'Report_Type': ['YTD'],
                    "DWP_12MM": DWP_12MM,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_DWP_Growth": YTD_DWP_Growth,
                    "YTD_NB_DWP": YTD_NB_DWP,
                    "Prior_YTD_NB_DWP": Prior_YTD_NB_DWP,
                    "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                    "NB_DWP_12MM": NB_DWP_12MM,
                    "YTD_Quotes": YTD_Quotes,
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    "YTD_Hit_Ratio": YTD_Hit_Ratio,
                    "Prior_YTD_Hit_Ratio": Prior_YTD_Hit_Ratio,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "YTD_PIF": YTD_PIF,
                    "YTD_Loss_Ratio" : YTD_Loss_Ratio,
                    "YTD_Quote_Growth": YTD_Quote_Growth

                    # add additional fields here
                }]
            }

            # now we make our member consolidated record.

            # this is our API call to the CRM API to post a record in the Member Consolidated Module.
            member_consolidated_count += 1
            consolidated_response = requests.post(
                url=consolidated_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))

            if (consolidated_response.json()['data'][0]['status'] == "error"):
                print("  ")
                print(request_body)
                print(consolidated_response.json())
                print("  ")
                return


    ## You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))
    return


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


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