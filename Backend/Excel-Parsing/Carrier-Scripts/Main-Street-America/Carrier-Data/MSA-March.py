from cmath import nan
from email import header
from operator import itemgetter
from threading import current_thread

from numpy import NaN, rec
import pandas as pd
from difflib import SequenceMatcher
import requests
import json


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

    carrier_data = pd.read_excel(
        'MSA - AC - Agency YTD by Locations March 2022.xlsx')

    zoho_account_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Accounts_001.csv')
    # zoho_agency_code_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Agency_Codes_C_001.csv')

    # create id dictionaries (maps)

    account_id_map = {}
    agency_code_id_map = {}


    # this is our zoho account data map, it will help us match agency codes we will have to create

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
        agency_code_id_map[current_record['Name']] = {
               'agency_code_id': current_record['id'],
                'acc_id': current_record['Account']['id'],
               }
        try:
            agency_code_id_map[current_record['Name']
                                   ]['contact_id'] = current_record['Contact']['id']

        except TypeError:
                agency_code_id_map[current_record['Name']]['contact_id'] = ''


    # this is the carrier report id of the AC Carrier Report record you created for this carrier report. If you haven't made one, you must
    # make one in order for this to work properly

    carrier_report_id = 5187612000001490001

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    match = 0
    lob_member_data = []

    # print(carrier_data)
    for i, row in carrier_data.loc[21:75].iterrows():
        count+=1
        print(count)
        # print(row)

        # print(row)
        # loop variables. These are reset every new row and must be filled in.
        # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        agency_name = str(row['Unnamed: 1']).strip()

        agency_code = str(row["Unnamed: 0"]).strip()

        name_string = agency_name + " - Main Street America - March 2022"

        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        # these are the variables that store the value of the corresponding cell.

        YTD_DWP = row["Unnamed: 6"]
        DWP_12MM = row["Unnamed: 10"]
        if type(row["Unnamed: 8"]) != str:
            YTD_DWP_Growth = round(row["Unnamed: 8"]*100, 2)
        else:
            YTD_DWP_Growth = 0
        YTD_NB_DWP_Growth = row["Unnamed: 2"]
        YTD_PIF = row["Unnamed: 14"]
        Prior_YTD_PIF = row["Unnamed: 14"] - row["Unnamed: 16"]
        Incurred_Loss_YTD = row["Unnamed: 23"]
        if type(row["Unnamed: 24"]) != str:
            YTD_Loss_Ratio = format_percentage(int(row["Unnamed: 24"]))
        else:
            YTD_Loss_Ratio = 0

        try:

            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]
            # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.
            # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names

            request_body = {
                "data": [
                    {
                        'Most_Recent': True,
                        'Report_Date': '2022-03-31',
                        'Carrier': 'Main Street America',
                        'Report_Type': ['YTD'],
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        'YTD_DWP': YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        'YTD_DWP_Growth': YTD_DWP_Growth,
                        'YTD_NB_DWP_Growth': YTD_NB_DWP_Growth,
                        'YTD_PIF': YTD_PIF,
                        'Prior_YTD_PIF': Prior_YTD_PIF,
                        'YTD_Loss_Ratio': YTD_Loss_Ratio
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
            formatted_agency_name = agency_name.strip(
            ).lower().replace(',', "").replace('.', "")

            try:
                account_id = account_id_map[formatted_agency_name]['acc_id']
                contact_id = account_id_map[formatted_agency_name]['cont_id']
            except KeyError:
                pass

        
            # When our code gets here, we either have an account matched or we don't. If we do, we proceed normally and make an agency code record and member consolidated record with the correct account and contact id. If not, we have an account for unassigned records named, you guessed it: 'Unassigned.'

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
                            "Carrier": "Main Street America",
                            "UAC_Name": agency_name,
                            "Contact": contact_id
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
                            'Most_Recent': True,
                            'Report_Date': '2022-03-31',
                            'Carrier': 'Main Street America',
                            'Report_Type': ['YTD'],
                            "Name": name_string,
                            "Carrier_Report": carrier_report_id,
                            "Account": account_id,
                            "Agency_Code": agency_code_id,
                            "Contact": contact_id,
                            'YTD_DWP': YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            'YTD_DWP_Growth': YTD_DWP_Growth,
                            'YTD_NB_DWP_Growth': YTD_NB_DWP_Growth,
                            'YTD_PIF': YTD_PIF,
                            'Prior_YTD_PIF': Prior_YTD_PIF,
                            'YTD_Loss_Ratio': YTD_Loss_Ratio
                        }
                    ]
                }

                # now we make our member consolidated record.

                # this is our API call to the CRM API to post a record in the Member Consolidated Module.

                consolidated_response = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

                if(consolidated_response.json()['data'][0]['status'] == "error"):

                    print("  ")
                    print(request_body)
                    print(agency_code)
                    print(consolidated_response.json())
                    print("  ")
                    return
        
            # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
            member_consolidated_id = consolidated_response.json()[
                'data'][0]['details']['id']
            member_consolidated_count += 1

           # You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))

    return


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

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
