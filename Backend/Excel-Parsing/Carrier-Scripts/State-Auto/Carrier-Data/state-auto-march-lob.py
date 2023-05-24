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

    carrier_data = pd.read_excel(
        'State Auto AC March 2022 - Personal Lines - Secondary Agt Code Report.xlsx')

    zoho_account_data = pd.read_csv(
        'C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Accounts_001.csv')
    # zoho_agency_code_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Agency_Codes_C_001.csv')


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

    carrier_report_id = 5187612000001614503

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    new_agency_code_count = 0

    count = 0
    match = 0
    lob_member_data = []

    carrier_data.columns = carrier_data.iloc[0]

    print("Starting on Personal Line of Business")
    # Personal Line of Business
    for i, row in carrier_data.loc[1:-1].iterrows():
        count+=1

        # Reset variables
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  

        name_code = row['SECONDARY AGENT CODE AND NAME']

        agency_name = str(name_code[8:]).strip()

        agency_code = str(name_code[:7])


        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        try:

            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

        except KeyError:
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


        # these are the variables that store the value of the corresponding cell.
        AC_Total_ID = 5187612000001614628
        lob_name_string = agency_name + \
        " - State Auto - March 2022 - PL"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Personal'
        YTD_DWP = int(row["CYTD WP"])
        Prior_YTD_DWP = int(row["PYTD WP"])
        DWP_12MM = int(row["CYTD R12 WP"])
        YTD_DWP_Growth = format_percentage(row["WP % Chg "])
        YTD_NB_DWP = int(row["CYTD NBWP"])
        Prior_YTD_NB_DWP = int(row["PYTD NBWP"])
        YTD_PIF = int(row["YTD PIF "])
        Prior_YTD_PIF = int(row["PYE PIF"])
        YTD_PIF_Growth = format_percentage(row["YTD PIF % Chg "])
        YTD_Loss_Ratio = format_percentage(row["CYTD Loss Ratio"])
   

        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Carrier': 'State Auto',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'YTD_PIF_Growth': YTD_PIF_Growth,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio
                }
            ]
        }
        # Post the record in Personal LOB 
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return


    # Commerical Line of Business
    carrier_data = pd.read_excel(
        'State Auto AC March 2022 Commercial Lines - Secondary Agent Code Report.xlsx')
    carrier_data.columns = carrier_data.iloc[0]

    print("Starting on Commercial Line of Business")
    for i, row in carrier_data.loc[1:118].iterrows():
        count+=1
        print(count)

        # loop variables. These are reset every new row and must be filled in.
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = '' 

        name_code = row['SECONDARY AGENT CODE AND NAME']

        agency_name = str(name_code[8:]).strip()

        agency_code = str(name_code[:7])


        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        try:

            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

        except KeyError:
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

        # these are the variables that store the value of the corresponding cell.
        AC_Total_ID = 5187612000001614633
        lob_name_string = agency_name + \
        " - State Auto - March 2022 - CL"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Commercial'
        YTD_DWP = int(row["CYTD WP"])
        Prior_YTD_DWP = int(row["PYTD WP"])
        DWP_12MM = int(row["CYTD R12 WP"])
        YTD_DWP_Growth = format_percentage(row["WP % Chg "])
        YTD_NB_DWP = int(row["CYTD NBWP"])
        Prior_YTD_NB_DWP = int(row["PYTD NBWP"])
        YTD_PIF = int(row["YTD PIF "])
        Prior_YTD_PIF = int(row["PYE PIF"])
        YTD_PIF_Growth = format_percentage(row["YTD PIF % Chg "])
        YTD_Loss_Ratio = format_percentage(row["CYTD Loss Ratio"])
   

        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Carrier': 'State Auto',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'YTD_PIF_Growth': YTD_PIF_Growth,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio
                }
            ]
        }
        # Post the record in Commercial LOB
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

    # Farm Line of Business
    carrier_data = pd.read_excel(
        'State Auto AC March 2022 Farm and Ranch Seondary Agt Code Report.xlsx')
    carrier_data.columns = carrier_data.iloc[0]

    print("Starting on Farm Line of Business")
    for i, row in carrier_data.loc[1:44].iterrows():
        count+=1
        print(count)

        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  
        member_consolidated_id = ''

        name_code = row['SECONDARY AGENT CODE AND NAME']

        agency_name = str(name_code[8:]).strip()

        agency_code = str(name_code[:7])


        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        try:
            # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

        except KeyError:
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
                            "Carrier": "State Auto",
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


        # these are the variables that store the value of the corresponding cell.
        AC_Total_ID = 5187612000001614638
        lob_name_string = agency_name + \
        " - State Auto - March 2022 - Farm"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Farm'
        YTD_DWP = int(row["CYTD WP"])
        Prior_YTD_DWP = int(row["PYTD WP"])
        DWP_12MM = int(row["CYTD R12 WP"])
        YTD_DWP_Growth = format_percentage(row["WP % Chg "])
        YTD_NB_DWP = int(row["CYTD NBWP"])
        Prior_YTD_NB_DWP = int(row["PYTD NBWP"])
        YTD_PIF = int(row["YTD PIF "])
        Prior_YTD_PIF = int(row["PYE PIF"])
        YTD_PIF_Growth = format_percentage(row["YTD PIF % Chg "])
        YTD_Loss_Ratio = format_percentage(row["CYTD Loss Ratio"])

        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Carrier': 'State Auto',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'YTD_PIF_Growth': YTD_PIF_Growth,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio
                }
            ]
        }
        # Post a record in the Farm LOB
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return


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
