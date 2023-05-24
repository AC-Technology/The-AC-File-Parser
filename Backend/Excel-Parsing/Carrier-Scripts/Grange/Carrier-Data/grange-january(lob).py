from calendar import c
from cmath import nan
from email import header
from email.quoprimime import quote
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

    carrier_data = pd.read_excel(
        'Grange Agency Collective 01 2022.xlsx',sheet_name="AGENCY COLLECTIVE")


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
        search_params = {'criteria': 'Carrier:equals:Grange',
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

    carrier_report_id = 5187612000001644759

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    match = 0
    lob_member_data = []


    carrier_data.columns = carrier_data.iloc[2]
    cols=pd.Series(carrier_data.columns)
    for dup in carrier_data.columns[carrier_data.columns.duplicated(keep=False)]: 
        cols[carrier_data.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(carrier_data.columns.get_loc(dup).sum())]
                                        )
    carrier_data.columns=cols

    for i, row in carrier_data.iloc[5:].iterrows():
        count+=1
        print(count)

        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''


        agency_name = row["Agency Name"]

        agency_code = row["Agency Number"]

        name_string = agency_name + " - Grange - January 2022"

        

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
                            "Carrier": "Grange",
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
    

        # Personal Line of Business
        print("Doing Personal Line of Business")

        ac_lob_total_id = 5187612000001644773
        # these are the variables that store the value of the corresponding cell.
        Report_Date = '2022-01-31'
        Line_of_Business = 'Personal'
        lob_name_string = agency_name + \
            " - Grange - January 2022 - PL"
        YTD_DWP = int(row["YTD DWP"])
        Prior_YTD_DWP = int(row["Prior YTD DWP"])
        DWP_12MM = int(row["Rolling 12 DWP"])
        Month_DWP = int(row["Month DWP"])
        YTD_NB_DWP = int(row['YTD NB'])
        Prior_YTD_NB_DWP = int(row['Prior YTD NB'])
        NB_DWP_12MM = int(row['Rolling 12 NB'])
        YTD_PIF = int(row['YTD PIF'])
        Prior_YTD_PIF = int(row['Prior YTD PIF'])
        Loss_Ratio_12MM = format_percentage(row['Rolling 12 LR'])
        YTD_Loss_Ratio = format_percentage(row['YTD LR'])
        YTD_Quotes = int(row['YTD Quotes'])
        Prior_YTD_Quotes = (row['Quote Count - Prior YTD'])
        Quotes_12MM = int(row['Rolling 12 Quotes'])
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Carrier': "Grange",
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

        # Commercial Line of Business
        print("Doing Starting Commerical Line of Business")
        ac_lob_total_id = 5187612000001644781
        # these are the variables that store the value of the corresponding cell.
        Report_Date = '2022-01-31'
        Line_of_Business = 'Commercial'
        lob_name_string = agency_name + \
            " - Grange - January 2022 - CL"
        YTD_DWP = int(row["YTD DWP.1"])
        Prior_YTD_DWP = int(row["Prior YTD DWP.1"])
        DWP_12MM = int(row["Rolling 12 DWP.1"])
        Month_DWP = int(row["Month DWP.1"])
        YTD_NB_DWP = int(row['YTD NB.1'])
        Prior_YTD_NB_DWP = int(row['Prior YTD NB.1'])
        NB_DWP_12MM = int(row['Rolling 12 NB.1'])
        YTD_PIF = int(row['YTD PIF.1'])
        Prior_YTD_PIF = int(row['Prior YTD PIF.1'])
        Loss_Ratio_12MM = format_percentage(row['Rolling 12 LR.1'])
        YTD_Loss_Ratio = format_percentage(row['YTD LR.1'])
        YTD_Quotes = int(row['YTD Quotes.1'])
        Prior_YTD_Quotes = (row['Prior YTD Quotes'])
        Quotes_12MM = int(row['Rolling 12 Quotes.1'])
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Carrier': "Grange",
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

        

        # Specialty Line of Business
        print("Doing Specialty Line of Business")
        ac_lob_total_id = 5187612000001644789
        # these are the variables that store the value of the corresponding cell.
        Report_Date = '2022-01-31'
        Line_of_Business = 'Specialty'
        lob_name_string = agency_name + \
            " - Grange - January 2022 - SL"
        YTD_DWP = int(row["YTD DWP.2"])
        Prior_YTD_DWP = int(row["Prior YTD DWP.2"])
        DWP_12MM = int(row["Rolling 12 DWP.2"])
        Month_DWP = int(row["Month DWP.2"])
        YTD_NB_DWP = int(row['YTD NB.2'])
        Prior_YTD_NB_DWP = int(row['Prior YTD NB.2'])
        NB_DWP_12MM = int(row['Rolling 12 NB.2'])
        YTD_PIF = int(row['YTD PIF.2'])
        Prior_YTD_PIF = int(row['Prior YTD PIF.2'])
        Loss_Ratio_12MM = format_percentage(row['Prior YTD Quotes.1'])
        YTD_Loss_Ratio = format_percentage(row['YTD LR.2'])
        YTD_Quotes = int(row['YTD Quotes.2'])
        Prior_YTD_Quotes = (row['Prior YTD Quotes.1'])
        Quotes_12MM = int(row['Rolling 12 Quotes.2'])
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Carrier': "Grange",
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP': YTD_DWP,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'DWP_12MM': DWP_12MM,
                    'Month_DWP': Month_DWP,
                    'YTD_NB_DWP': YTD_NB_DWP,
                    'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                    'NB_DWP_12MM': NB_DWP_12MM,
                    'YTD_PIF': YTD_PIF,
                    'Prior_YTD_PIF': Prior_YTD_PIF,
                    'Loss_Ratio_12MM': Loss_Ratio_12MM,
                    'YTD_Loss_Ratio': YTD_Loss_Ratio,
                    'YTD_Quotes': YTD_Quotes,
                    'Prior_YTD_Quotes': Prior_YTD_Quotes,
                    'Quotes_12MM': Quotes_12MM
                }
            ]
        }
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

     # You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    # print('Consolidated records created: ' + str(member_consolidated_count))
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
