from cmath import nan
from email import header
from operator import itemgetter
from threading import current_thread
from unicodedata import name

from numpy import NaN, rec
import pandas as pd
from difflib import SequenceMatcher
import requests
import json

# this template is for putting spreadsheet data into the CRM via the API. In web dev terms: we are making a post request to the CRM API with a mapped json object.

# This isn't a complicated process at all, the heavy lifting is just finding out how to map it properly so it's all linked together and organized. That's what this template tries to help you do.

# 1. Authentication. We can't do anything until zoho gives us stuff to access the user's crm.
# 2. Prepare maps and variables for loop
# 3. Loop through carrier data and insert it into CRM via the API

# If you need reference for how all data will look in the system. Guard and State Auto carrier data is already in the system correctly.

# GLOBAL VARIABLES
# Carrier Report ID
# This will come from the 'Foremost - March 2022' AC Carrier Report URL
carrier_report_id = 5187612000001818299
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
    print(auth_response.json())

    # Turn the response into readable json and store the access token in a variable
    access_token = auth_response.json()['access_token']
    print(access_token)

    # this authorization header will be used to make API calls. This is specified in the Zoho CRM documentation.
    # https://www.zoho.com/crm/developer/docs/api/v2/insert-records.html
    # it would be helpful to have this open in a tab.

    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    # Read Carrier Data
    carrier_data = pd.read_excel('Foremost\Carrier-Data\Foremost Signature Broker Breakdown_The AC_033122.xlsx', 'Sheet1')
    # Read Zoho Account Data
    zoho_account_data = pd.read_csv('Zoho-Data\Accounts_001.csv')

    # Maps
    account_id_map = {}
    agency_code_id_map = {}


    # Loop through Zoho Account Data
    for i, row in zoho_account_data.iterrows():
        # this horrific piece of string manipulation removes trailing space, puts everything in lowercase, and removes all periods and commas.
        account_name = str(row['Account Name']).strip(
        ).lower().replace(',', "").replace('.', "")

        account_id_map[account_name] = {

            'acc_id': str(row['Record Id']).split('_')[1],

        }
        try:
            account_id_map[account_name]['cont_id'] = str(
                row['Primary Contact ID']).split('_')[1]
        except IndexError:
            account_id_map[account_name]['cont_id'] = ''




    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Foremost', 'page':page
        }
        agency_get_response = requests.get(url=agency_code_search_url,
                                        headers=headers,
                                        params=search_params)
        for i in range(len(agency_get_response.json()['data'])):
            agency_code_records.append(agency_get_response.json()['data'][i])
        if(not agency_get_response.json()['info']['more_records']):
            more_records = False
        page +=1

    print(agency_code_records)
    print(len(agency_code_records))

    # Loop through Agency Code Records
    for i in range(len(agency_code_records)):
        current_record = agency_code_records[i]
        agency_code_id_map[current_record['Name']] = {
               'agency_code_id': current_record['id'],
                'acc_id': current_record['Account']['id'],
               }
        try:
            agency_code_id_map[current_record['Name']
                                   ]['contact_id'] = current_record['Contact']['id']

        except TypeError:
                agency_code_id_map[current_record['Name']]['contact_id'] = ''
    print("Agency Code ID Map")
    print(agency_code_id_map)

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    lob_member_data = []
    print(carrier_data.columns)


    for i, row in carrier_data[1:67].iterrows():
        # loop variables. These are reset every new row and must be filled in.
        # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''
        
        # "Agent/Broker Name" Column
        agency_name = str(row['Agent/ Broker Name']).strip()
        print(agency_name)
        # "Agent/Broker ID" Column
        agency_code = str(row["Agent/ Broker ID"]).strip()
        # Create Name String
        name_string = agency_name + " - Foremost - March 2022"
        # "YTD Quotes" Column
        YTD_Quotes = int(row["YTD Quotes"])
        print(YTD_Quotes)
        # "YTD New WP" Column: Contains Year to Date New Business Written Premium
        YTD_NB_DWP = is_integer_num(row['YTD New WP'])
        # Year to Date Direct Written Premium = 2022 YTD Total WP(Written Premium)
        YTD_DWP = is_integer_num(row["YTD Total WP"])
        # "YTD Total Incurred Loss Calendar Year" Column:
        Incurred_Loss_YTD = is_integer_num(row["YTD Total Incurred Loss Calendar Year"])
        # "PYTD Quotes" Column: Contains  Prior Year to Date Quotes
        Prior_YTD_Quotes = int(row["PYTD Quotes"])

        YTD_Submits = int(row["YTD Submitted Policies"])

        Prior_YTD_Submits = int(row["PYTD Submitted Policies"])

        

        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}
        print(YTD_DWP)

        try:
            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables

            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

            # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.

            # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names

        except KeyError:
            # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

            # two things can happen here when you're looking at a new carrier dataset: It has a UAC number column, and it doesn't.

            # if they have a UAC number column, you MUST insert another Try-Except statement INSIDE this KeyError exception. This try except statement tries to map the UAC number to the zoho agency code data. If a match using the UAC number is found in the zoho agency code data, then a new agency code is created with that UAC number as a new agency code and the relevant associated data.

            # if they do not have a UAC number column, then you DO NOT NEED another try-except statement.

            # This template assumes that there IS NOT a UAC column. If you run into a dataset with a UAC column, look at nationwide.py. It's not commented, so feel free to ask me if you need help.
            # Since no agency code match was found, we are going to have to try and find an account and contact id to match to the new agency code record we are going to create. We'll loop through the account id map and see if the given agency name on this row of carrier data is in they key we are looping through and vice versa.

            # before we put it through the loop, we'll use our horrific piece of string concatenation to try and lower our missing matches. If you don't think this is enough, use the similar function written below the main function to find a good ratio.

            formatted_agency_name = agency_name.strip().lower().replace(',', "").replace('.', "")
            try:
                account_id = account_id_map[formatted_agency_name]['acc_id']
                contact_id = account_id_map[formatted_agency_name]['cont_id']
            except KeyError:
                pass

            # When our code gets here, we either have an account matched or we don't. If we do, we proceed normally and make an agency code record and member consolidated record with the correct account and contact id. If not, we have an account for unassigned records named, you guessed it: 'Unassigned.'
            if (account_id == ''):
                # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.
                account_id = 5187612000000560802
                # we found an account id match so now we just need to create our agency code record.

            request_body = {
                "data": [{
                    "Account": account_id,
                    "Name": agency_code,
                    "UAC": uac_number,
                    "Carrier": "Foremost",
                    "UAC_Name": agency_name,
                    "Contact": contact_id
                    # add additional fields here
                }]
            }

            # This is our API call to the CRM API to post a record in the Agency Code Module.
            agency_code_response = requests.post(
                url=agency_code_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))

            # Note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.
            if (agency_code_response.json()['data'][0]['status'] == "error"):
                print("  ")
                print(request_body)
                print(agency_code_response.json())
                print("  ")
                return
            agency_code_id = agency_code_response.json()['data'][0]['details']['id']
                # If it returns a successful response, then the created agency code record id is stored into the corresponding variable
        request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier_Report": carrier_report_id,
                    "Account": account_id,
                    "Agency_Code": agency_code_id,
                    "Contact": contact_id,
                    "Carrier": "Foremost",
                    # YTD Quotes
                    "YTD_Quotes": YTD_Quotes,
                    "Report_Date": "2022-03-31",
                    'Report_Type': ['Monthly','YTD'],
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    # Year to Date Direct Written Premium = 2022 YTD Total WP(Written Premium)
                    "YTD_DWP": YTD_DWP,
                    # New Written Premium YTD
                    "YTD_NB_DWP": YTD_NB_DWP,
                    #YTD Incurred Loss
                    "Incurred_Loss_YTD": Incurred_Loss_YTD,

                    "YTD_Submits": YTD_Submits,

                    "Prior_YTD_Submits": Prior_YTD_Submits
            }]
        }

        # This is our API call to the CRM API to post a record in the Member Consolidated Module.
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

def is_integer_num(n):
    if type(n)== int:
        return n
    if type(n)== float:
        return round(n, 1)

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