from calendar import c
from cmath import nan
from email import header
from operator import itemgetter
from threading import current_thread

from numpy import NaN, mat, rec
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

client_id = '1000.G01B0UNFGTFK5HSMYSO8QCBP5R5LKX'
client_secret = '262eec9c5c47b597f5666fc78ee6369912fde29bfc'
refresh_token = '1000.b6365733e341a5c2c8617643aaeecd99.017bf902178c60dd7f76448770b2deca'

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

    carrier_data = pd.read_excel('AC 2021 State Auto All Agents Production Report.xlsx')

    zoho_account_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Accounts_001.csv')
    # zoho_agency_code_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Agency_Codes_C_001.csv')


    account_id_map = {}
    agency_code_id_map = {}

   

    for i, row in zoho_account_data.iterrows():
        # this horrific piece of string manipulation removes trailing space, puts everything in lowercase, and removes all periods and commas.
        account_name = str(row['Account Name']).strip().lower().replace(',', "").replace('.', "")
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
        search_params = {'criteria': 'Carrier:equals:State Auto',
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
        agency_code_id_map[current_record['Name']] = {
               'agency_code_id': current_record['id'],
                'acc_id': current_record['Account']['id'],
               }
        try:
            agency_code_id_map[current_record['Name']
                                   ]['contact_id'] = current_record['Contact']['id']

        except TypeError:
                agency_code_id_map[current_record['Name']]['contact_id'] = ''




    # this is the carrier report id of the AC Carrier Report record you created for Wthis carrier report. If you haven't made one, you must
    # make one in order for this to work properly

    carrier_report_id = 5187612000002425001

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    match = 0
    lob_member_data = []

    # print(carrier_data)

    # This is to see what codes match up after using the search api to add agency codes to the map

    match = 0
    count = 0

    # print(carrier_data)
    # for i, row in carrier_data.loc[0:146].iterrows():
    #     # print(row)
    #     # loop variables. These are reset every new row and must be filled in.
    #     # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
    #     account_id = ''
    #     contact_id = ''
    #     agency_code_id = ''
    #     uac_number = ''  # if needed
    #     # member_consolidated_id = ''

    #     name_code = row['SECONDARY AGENT CODE AND NAME']

    #     agency_name = str(name_code[8:]).strip()

    #     agency_code = str(name_code[:7])
    #     print(agency_code)

    #     try:
    #         test = agency_code_id_map[agency_code]
    #         match += 1
    #     except KeyError:
    #         count += 1
    # print("Matched: ",match)
    # print("Missing: ",count)






    for i, row in carrier_data.loc[0:146].iterrows():
        count+=1
        print(count)

        # print(row)
        # loop variables. These are reset every new row and must be filled in.
        # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        name_code = row['SECONDARY AGENT CODE AND NAME']

        agency_name = str(name_code[8:]).strip()

        agency_code = str(name_code[:7])

        name_string = agency_name + " - State Auto - December 2021"

        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}


        # these are the variables that store the value of the corresponding cell.
        Report_Date = '2021-12-31'
        YTD_DWP = int(row["CYTD WP"])
        Prior_YTD_DWP = int(row["PYTD WP"])
        DWP_12MM = int(row["CYTD R12 WP"])
        # Month_DWP =
        YTD_DWP_Growth = format_percentage(row["WP % Chg "])
        # Earned_Premium_12MM =
        # Primary_Contact =
        YTD_NB_DWP = int(row["CYTD NBWP"])
        Prior_YTD_NB_DWP = int(row["PYTD NBWP"])
        # NB_DWP_12MM =
        YTD_NB_DWP_Growth = format_percentage(row['NBWP % Chg '])
        YTD_PIF = int(row["YTD PIF "])
        # Prior_YTD_PIF = int(row["PYE PIF"])
        # PIF_12MM =
        YTD_PIF_Growth = format_percentage(row["YTD PIF % Chg "])
        # Incurred_Loss_YTD = 
        # Incurred_Loss_12MM =
        # Capincurred_Loss_12MM =
        # Loss_Ratio_12MM =
        YTD_Loss_Ratio = format_percentage(row["CYTD Loss Ratio"])
        # YTD_Quotes =
        # Prior_YTD_Quotes =
        # Quotes_12MM =
        # YTD_Quote_Growth =


        try:

            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]


            request_body = {
                "data": [
                    {
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Carrier': "State Auto",
                        "Carrier_Report": carrier_report_id,
                        'Name': name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'YTD_DWP': YTD_DWP,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        'YTD_DWP_Growth': YTD_DWP_Growth,
                        'YTD_NB_DWP_Growth': YTD_NB_DWP_Growth,
                        'YTD_NB_DWP': YTD_NB_DWP,
                        'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                        'YTD_PIF': YTD_PIF,
                        'YTD_PIF_Growth': YTD_PIF_Growth,
                        'YTD_Loss_Ratio': YTD_Loss_Ratio

                        # add additional fields here


                    }
                ]
            }

            # # this is our API call to the CRM API to post a record in the Member Consolidated Module.

            consolidated_response = requests.post(
                url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

            # if there is an error, the code will end and it prints the request body and the response

            if(consolidated_response.json()['data'][0]['status'] == "error"):
                member_consolidated_count +=1
                print("  ")
                print(request_body)
                print(consolidated_response.json())
                print("  ")
                return
            else:
                # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
                member_consolidated_id = consolidated_response.json()[
                    'data'][0]['details']['id']
                member_consolidated_count += 1


        except KeyError:
            # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

            # two things can happen here when you're looking at a new carrier dataset: It has a UAC number column, and it doesn't.

            # if they have a UAC number column, you MUST insert another Try-Except statement INSIDE this KeyError exception. This try except statement tries to map the UAC number to the zoho agency code data. If a match using the UAC number is found in the zoho agency code data, then a new agency code is created with that UAC number as a new agency code and the relevant associated data.

            # if they do not have a UAC number column, then you DO NOT NEED another try-except statement.

            # This template assumes that there IS NOT a UAC column. If you run into a dataset with a UAC column, look at nationwide.py. It's not commented, so feel free to ask me if you need help.
            # Since no agency code match was found, we are going to have to try and find an account and contact id to match to the new agency code record we are going to create. We'll loop through the account id map and see if the given agency name on this row of carrier data is in they key we are looping through and vice versa.

            # before we put it through the loop, we'll use our horrific piece of string concatenation to try and lower our missing matches. If you don't think this is enough, use the similar function written below the main function to find a good ratio.

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
                            "Carrier": "State Auto",
                            "UAC_Name": agency_name,
                            "Contact": contact_id
                            # add additional fields here
                        }
                    ]
                }
                # this is our API call to the CRM API to post a record in the Agency Code Module.

                agency_code_response = requests.post(
                    url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
                print(agency_code_response.request)

                # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.
                print(request_body)
                print(agency_code_response.json())

                if(agency_code_response.json()['data'][0]['status'] == "error"):

                    print("  ")
                
                    print("  ")
                    return
                
                
                # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

                agency_code_id = agency_code_response.json()[
                    'data'][0]['details']['id']
                new_agency_code_count += 1

                request_body = {
                "data": [
                    {
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Carrier':"State Auto",
                        "Carrier_Report": carrier_report_id,
                        'Name': name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'YTD_DWP': YTD_DWP,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        'YTD_DWP_Growth': YTD_DWP_Growth,
                        'YTD_NB_DWP': YTD_NB_DWP,
                        'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                        'YTD_PIF': YTD_PIF,
                        'YTD_PIF_Growth': YTD_PIF_Growth,
                        'YTD_Loss_Ratio': YTD_Loss_Ratio

                        # add additional fields here


                    }
                ]
            }

            # # this is our API call to the CRM API to post a record in the Member Consolidated Module.

            consolidated_response = requests.post(
                url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

            # if there is an error, the code will end and it prints the request body and the response

            if(consolidated_response.json()['data'][0]['status'] == "error"):
                member_consolidated_count +=1
                print("  ")
                print(request_body)
                print(consolidated_response.json())
                print("  ")
                return
            else:
                # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
                member_consolidated_id = consolidated_response.json()[
                    'data'][0]['details']['id']
                member_consolidated_count += 1

            # You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))
          

            


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
