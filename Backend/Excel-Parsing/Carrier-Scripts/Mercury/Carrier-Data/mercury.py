from cmath import nan
from email import header

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

# these are the client id, secret, and the refresh token you will be using to make API calls to the CRM API
# they are given to us by zoho and are unique to every CRM
# The way authentication with zoho is laid out is on the board behind Tran's desk.

client_id = '1000.WI69YDZMVKOI2OVL22NID700CTMNTD'
client_secret = '9689b45819ebb9b16ffab2d96ae9108ebefee9eae5'
refresh_token = '1000.92c7d1df11e9e5b8d259c7edf73fdb88.e918f39048f8f838075705efcd73c28c'

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

    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    # read carrier and zoho data in using pandas

    carrier_data = pd.read_excel('Mercury - The AC - Nov 2021(1).xlsx')

    zoho_account_data = pd.read_csv('../../Zoho-Data/Accounts_001.csv')
    zoho_agency_code_data = pd.read_csv('../../Zoho-Data/Agency_Codes_C_001.csv')

    # create id dictionaries (maps)

    account_id_map = {}
    agency_code_id_map = {}

    # The goal of these dictionaries is to be able to link the data that needs to be put into the system (carrier data),
    # with the data that is already in the system (zoho data). We could use a double for-loop, but this is faster.
    #
    # Remember from the module layout behind Hugo's desk (RIP), the number of 'one to many' indicators is an indicator of how
    # many modules the module you are uploading a record to can link to. When uploading the carrier data, there are
    # different modules that you must link to, with the data already being in the system (zoho data).
    # To link a record from one module to a record in another module, we use a lookup field in zoho.
    # When uploading records via the CRM API we input a zoho record id into the lookup field, and the two records become associated.
    #
    # When you're looping through carrier data, you'll use these to find the correct record id's to link to when uploading your carrier data record.
    #

    # loop through zoho agency code data
    # for i, row in zoho_agency_code_data.iterrows():

        # you only want agency codes from the carrier you are working with, so replace INSERT_CARRIER_NAME with the correct name

    #     if(row["Carrier"] == "INSERT_CARRIER_NAME"):

    #         # this extracts the Account ID using some string manipulation.
    #         # to see why this is necessary open the spreadsheet in your terminal by entering
    #         # 'open Accounts_001.csv' and looking at the Agency ID column.
    #         account_id = str(row["Agency ID"]).split("_")[1]

    #         # strip the agency code of any whitespace
    #         agency_code = str(row["Agency Code"]).strip()

    #         # this creates a new key in the agency code id map and stores the account id, agency code id, and contact id

    #         # if you were to print this specific key:value out out, it would look like this
    #         # {
    #         # note that these numbers are just for example
    #         #   '4893029': {
    #         #       'acc_id': 5784930293487508430,
    #         #       'agency_code_id': 2343289790923879
    #         #       'cont_id': 58490303948500948
    #         #   }
    #         #
    #         # }

    #         #
    #         agency_code_id_map[agency_code] = {
    #             "acc_id": account_id,
    #             "agency_code_id": str(row["Record Id"]).split("_")[1],
    #         }
    #         # not every agency code has a contact id associated with it, so we have
    #         # to use a try except so our code doesn't break when it tries to perform this string manipulation. The error is an IndexError
    #         # It would be to your benefit seeing the error, so i highly recommend taking this part code out of the
    #         # try except statement and trying to run it
    #         try:
    #             agency_code_id_map[agency_code]['contact_id'] = str(
    #                 row["Agency Contact ID"]).split("_")[1]

    #         except IndexError:
    #             agency_code_id_map[agency_code]['contact_id'] = ""

    # # uncomment this and print the agency code id map to see it
    # # print("Agency Code ID Map")
    # # print(agency_code_id_map)

    # # this is our zoho account data map, it will help us match agency codes we will have to create

    # for i, row in zoho_account_data.iterrows():
    #     # this horrific piece of string manipulation removes trailing space, puts everything in lowercase, and removes all periods and commas.
    #     account_name = str(row['Account Name']).strip(
    #     ).lower().replace(',', "").replace('.', "")

    #     account_id_map[account_name] = {

    #         'acc_id': str(row['Record Id']).split('_')[1],

    #     }
    #     try:
    #         account_id_map[account_name]['cont_id'] = str(
    #             row['Primary Contact ID']).split('_')[1]
    #     except IndexError:
    #         account_id_map[account_name]['cont_id'] = ''

    # # this is the carrier report id of the AC Carrier Report record you created for this carrier report. If you haven't made one, you must
    # # make one in order for this to work properly

    # carrier_report_id = 5187612000001148185

    # # these are counts displayed at the end of the code,
    # # they are just to help keep count of what you are making

    # member_consolidated_count = 0
    # new_agency_code_count = 0

    # count = 0
    # lob_member_data = []

    for i, row in carrier_data.iterrows():
        print(row['Master Agency'])
    #     # loop variables. These are reset every new row and must be filled in.
    #     # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
    #     account_id = ''
    #     contact_id = ''
    #     agency_code_id = ''
    #     uac_number = ''  # if needed
    #     member_consolidated_id = ''

    #     agency_name = str(row['Insert Column Here']).strip()

    #     agency_code = str(row["Insert Column Name"]).strip()
    #     name_string = agency_name + " - CARRIER - Month YYYY"

    #     # this dictionary will hold our API request data when we're ready to send it
    #     request_body = {}

    #     # these are the variables that store the value of the corresponding cell.

    #     YTD_DWP = row["Insert Column Name"]
    #     Prior_YTD_DWP = row["Insert Column Name"]
    #     DWP_12MM = row["Insert Column Name"]

    #     try:

    #         # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables

    #         account_id = agency_code_id_map[agency_code]["acc_id"]
    #         agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
    #         contact_id = agency_code_id_map[agency_code]["contact_id"]

    #         # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.

    #         # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names

    #         request_body = {
    #             "data": [
    #                 {
    #                     "Name": name_string,
    #                     "Carrier_Report": carrier_report_id,
    #                     "Account": account_id,
    #                     "Agency_Code": agency_code_id,
    #                     "Contact": contact_id,
    #                     'YTD_DWP': YTD_DWP
    #                     # add additional fields here


    #                 }
    #             ]
    #         }

    #         # this is our API call to the CRM API to post a record in the Member Consolidated Module.

    #         consolidated_response = requests.post(
    #             url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

    #         # if there is an error, the code will end and it prints the request body and the response

    #         if(consolidated_response.json()['data'][0]['status'] == "error"):
    #             print("  ")
    #             print(request_body)
    #             print(consolidated_response.json())
    #             print("  ")
    #             return
    #         else:
    #             # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
    #             member_consolidated_id = consolidated_response.json()[
    #                 'data'][0]['details']['id']
    #             member_consolidated_id += 1

    #     except KeyError:
    #         # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

    #         # two things can happen here when you're looking at a new carrier dataset: It has a UAC number column, and it doesn't.

    #         # if they have a UAC number column, you MUST insert another Try-Except statement INSIDE this KeyError exception. This try except statement tries to map the UAC number to the zoho agency code data. If a match using the UAC number is found in the zoho agency code data, then a new agency code is created with that UAC number as a new agency code and the relevant associated data.

    #         # if they do not have a UAC number column, then you DO NOT NEED another try-except statement.

    #         # This template assumes that there IS NOT a UAC column. If you run into a dataset with a UAC column, look at nationwide.py. It's not commented, so feel free to ask me if you need help.
    #         # Since no agency code match was found, we are going to have to try and find an account and contact id to match to the new agency code record we are going to create. We'll loop through the account id map and see if the given agency name on this row of carrier data is in they key we are looping through and vice versa.

    #         # before we put it through the loop, we'll use our horrific piece of string concatenation to try and lower our missing matches. If you don't think this is enough, use the similar function written below the main function to find a good ratio.

    #         formatted_agency_name = agency_name.strip(
    #         ).lower().replace(',', "").replace('.', "")

    #         for key in account_id_map:
    #             if(formatted_agency_name in key):
    #                 account_id = account_id_map[key]['acc_id']
    #                 contact_id = account_id_map[key]['cont_id']
    #             if(key in formatted_agency_name):
    #                 account_id = account_id_map[key]['acc_id']
    #                 contact_id = account_id_map[key]['cont_id']

    #         # When our code gets here, we either have an account matched or we don't. If we do, we proceed normally and make an agency code record and member consolidated record with the correct account and contact id. If not, we have an account for unassigned records named, you guessed it: 'Unassigned.'

    #         if(account_id == ''):
    #             # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.

    #             account_id = 0

    #             request_body = {
    #                 "data": [
    #                     {
    #                         "Account": account_id,
    #                         "Name": agency_code,
    #                         "UAC": uac_number,
    #                         "Carrier": "Nationwide",
    #                         "UAC_Name": agency_name,
    #                         "Contact": contact_id
    #                         # add additional fields here
    #                     }
    #                 ]
    #             }
    #             # this is our API call to the CRM API to post a record in the Agency Code Module.

    #             agency_code_response = requests.post(
    #                 url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

    #             # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.

    #             if(agency_code.json()['data'][0]['status'] == "error"):

    #                 print("  ")
    #                 print(request_body)
    #                 print(agency_code_response.json())
    #                 print("  ")
    #                 return
    #             else:
    #                 # if it returns a successful response, then the created agency code id is stored into the corresponding variable

    #                 agency_code_id = agency_code_response.json()[
    #                     'data'][0]['details']['id']
    #                 new_agency_code_count += 1
    #                 request_body = {
    #                     "data": [
    #                         {
    #                             "Name": name_string,
    #                             "Carrier_Report": carrier_report_id,
    #                             "Account": account_id,
    #                             "Agency_Code": agency_code_id,
    #                             "Contact": contact_id,
    #                             # add additional fields here

    #                         }
    #                     ]
    #                 }
    #                 consolidated_response = requests.post(
    #                     url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

    #                 if(consolidated_response.json()['data'][0]['status'] == "error"):

    #                     print("  ")
    #                     print(request_body)
    #                     print(consolidated_response.json())
    #                     print("  ")
    #                     return
    #                 else:
    #                     # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
    #                     member_consolidated_id = consolidated_response.json()[
    #                         'data'][0]['details']['id']
    #                     member_consolidated_id += 1

    #         else:
    #             # we found an account id match so now we just need to create our agency code record.

    #             request_body = {
    #                 "data": [
    #                     {
    #                         "Account": account_id,
    #                         "Name": agency_code,
    #                         "UAC": uac_number,
    #                         "Carrier": "Nationwide",
    #                         "UAC_Name": agency_name,
    #                         "Contact": contact_id
    #                         # add additional fields here
    #                     }
    #                 ]
    #             }
    #             # this is our API call to the CRM API to post a record in the Agency Code Module.

    #             agency_code_response = requests.post(
    #                 url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

    #             # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.

    #             if(agency_code.json()['data'][0]['status'] == "error"):

    #                 print("  ")
    #                 print(request_body)
    #                 print(agency_code_response.json())
    #                 print("  ")
    #                 return
    #             else:
    #                 # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

    #                 agency_code_id = consolidated_response.json()[
    #                     'data'][0]['details']['id']
    #                 new_agency_code_count += 1
    #                 request_body = {
    #                     "data": [
    #                         {
    #                             "Name": name_string,
    #                             "Carrier_Report": carrier_report_id,
    #                             "Account": account_id,
    #                             "Agency_Code": agency_code_id,
    #                             "Contact": contact_id,
    #                             # add additional fields here

    #                         }
    #                     ]
    #                 }

    #             # now we make our member consolidated record.

    #             # this is our API call to the CRM API to post a record in the Member Consolidated Module.

    #                 consolidated_response = requests.post(
    #                     url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

    #                 if(consolidated_response.json()['data'][0]['status'] == "error"):

    #                     print("  ")
    #                     print(request_body)
    #                     print(consolidated_response.json())
    #                     print("  ")
    #                     return
    #                 else:
    #                     # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
    #                     member_consolidated_id = consolidated_response.json()[
    #                         'data'][0]['details']['id']
    #                     member_consolidated_id += 1

    #        ## You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    # print('Consolidated records created: '+ str(member_consolidated_count))
    # print('Agency Codes created: '+ str(new_agency_code_count))
    # return


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()





main()
