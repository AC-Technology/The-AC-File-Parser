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
        'Safeco Mar 2022 Agency Collective_Trend_Report.xlsx',sheet_name="Location")

    zoho_account_data = pd.read_csv(
        'C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Accounts_001.csv')
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

    carrier_report_id = 5187612000002144889

    # these are counts displayed at the end of the code,
    # they are just to help keep count of what you are making

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    match = 0
    lob_member_data = []

    # This is to see what codes match up after using the search api to add agency codes to the map

    match = 0
    count = 0

    carrier_data.columns = carrier_data.loc[5]

    carrier_data = carrier_data.fillna("")
    for i, row in carrier_data.loc[6:].iterrows():
        count+=1
        print(count)

        # reset variables
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        agency_name = str(row['Agency Name']).strip()

        agency_code = str(row["SubStat"]).strip()

        uac_number = str(row["Location ID"]).strip()

        name_string = agency_name + " - Safeco - March 2022"

        # this dictionary will hold our API request data when we're ready to send it
        request_body = {}

        # these are the variables that store the value of the corresponding cell.
        Report_Date = '2022-03-31'
        YTD_DWP = int(row["Written Premium"])
        if row["Written Premium PY"] != "":
            Prior_YTD_DWP = int(row["Written Premium PY"])
        else:
            Prior_YTD_DWP = row["Written Premium PY"]
        DWP_12MM = int(row["R12 Written Premium"])
        YTD_DWP_Growth = format_percentage(row["Written Premium Growth"])
        YTD_NB_DWP = int(row['New Premium'])
        if row["New Premium PY"] != "":
            Prior_YTD_NB_DWP = int(row['New Premium PY'])
        else:
            Prior_YTD_NB_DWP = row["New Premium PY"]
        NB_DWP_12MM = int(row["R12 New Premium"])
        YTD_NB_DWP_Growth = format_percentage(row['New Premium Growth'])
        YTD_PIF = int(row["PIF"])
        if row["PIF PY"] != "":
            Prior_YTD_PIF = int(row["PIF PY"])
        else:
            Prior_YTD_PIF = row["PIF PY"]
        YTD_PIF_Growth = format_percentage(row["PIF Growth"])
        Incurred_Loss_YTD = int(row["YTD Incurred Losses"])
        YTD_Loss_Ratio = format_percentage(row["Loss Ratio"])
        YTD_Hit_Ratio = format_percentage(row["Close Ratio YTD"])
        Prior_YTD_Hit_Ratio = format_percentage(row["Close Ratio PY"])
        YTD_Quotes = int(row["Quotes YTD"])
        if row["Quotes PY"] != "":
            Prior_YTD_Quotes = int(row["Quotes PY"])
        else:
            Prior_YTD_Quotes = row["Quotes PY"]
        YTD_Quote_Growth = format_percentage(row["Quote Growth"])


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
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Uniform_Agency_Code': uac_number,
                        'Carrier': "Safeco",
                        "Carrier_Report": carrier_report_id,
                        'Name': name_string,
                        'Report_Type': ['Monthly','YTD'],
                        'Report_Date': Report_Date,
                        'YTD_DWP': YTD_DWP,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        'YTD_DWP_Growth': YTD_DWP_Growth,
                        'YTD_PIF': YTD_PIF,
                        'Prior_YTD_PIF': Prior_YTD_PIF,
                        'YTD_PIF_Growth': YTD_PIF_Growth,
                        'Incurred_Loss_YTD': Incurred_Loss_YTD,
                        'YTD_Loss_Ratio': YTD_Loss_Ratio,
                        'YTD_Quotes': YTD_Quotes,
                        'Prior_YTD_Quotes': Prior_YTD_Quotes,
                        'YTD_Quote_Growth': YTD_Quote_Growth,
                        'YTD_NB_DWP' : YTD_NB_DWP,
                        'Prior_YTD_NB_DWP' : Prior_YTD_NB_DWP,
                        'NB_DWP_12MM' : NB_DWP_12MM,
                        'YTD_NB_DWP_Growth': YTD_NB_DWP_Growth,
                        'YTD_Hit_Ratio': YTD_Hit_Ratio,
                        'Prior_YTD_Hit_Ratio': Prior_YTD_Hit_Ratio
                        

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
                        "Carrier": "Safeco",
                        "UAC_Name": agency_name,
                        "Contact": contact_id
                    }
                ]
            }

            # this is our API call to the CRM API to post a record in the Agency Code Module.

            agency_code_response = requests.post(
                url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
            

            # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.
            print(request_body)
            print(agency_code_response.json())

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


            request_body = {
                "data": [
                    {
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Uniform_Agency_Code': uac_number,
                        'Carrier': "Safeco",
                        "Carrier_Report": carrier_report_id,
                        'Name': name_string,
                        'Report_Type': ['Monthly','YTD'],
                        'Report_Date': Report_Date,
                        'YTD_DWP': YTD_DWP,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        'YTD_PIF': YTD_PIF,
                        'Prior_YTD_PIF': Prior_YTD_PIF,
                        'YTD_PIF_Growth': YTD_PIF_Growth,
                        'Incurred_Loss_YTD': Incurred_Loss_YTD,
                        'YTD_Loss_Ratio': YTD_Loss_Ratio,
                        'YTD_Quotes': YTD_Quotes,
                        'Prior_YTD_Quotes': Prior_YTD_Quotes,
                        'YTD_Quote_Growth': YTD_Quote_Growth,
                        'YTD_Hit_Ratio': YTD_Hit_Ratio,
                        'Prior_YTD_Hit_Ratio': Prior_YTD_Hit_Ratio

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


        # This is Auto  
        AC_Total_ID = 5187612000002342001
        lob_name_string = agency_name + \
        " - Safeco - March 2022 - Auto"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Auto'
        YTD_DWP = row["Auto Written Premium"]
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Consolidated_Record': member_consolidated_id,
                    'Carrier': 'Safeco',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP' : YTD_DWP
                }
            ]
        }
        # Post the record in Auto LOB
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

        # This is Home
        AC_Total_ID = 5187612000002342016
        lob_name_string = agency_name + \
        " - Safeco - March 2022 - Home"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Home'
        YTD_DWP = row["Home Written Premium"]
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Consolidated_Record': member_consolidated_id,
                    'Carrier': 'Safeco',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP' : YTD_DWP
                }
            ]
        }
        # Post the record in Home LOB
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return

        # This is Specialty
        AC_Total_ID = 5187612000002342021
        lob_name_string = agency_name + \
        " - Safeco - March 2022 - Specialty"
        Report_Date = '2022-03-31'
        Line_of_Business = 'Specialty'
        YTD_DWP = row["Specialty Written Premium"]
        request_body = {
            "data": [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Consolidated_Record': member_consolidated_id,
                    'Carrier': 'Safeco',
                    'Agency_Code': agency_code_id,
                    'AC_Total': AC_Total_ID,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','YTD'],
                    'Report_Date': Report_Date,
                    'Line_of_Business': Line_of_Business,
                    'YTD_DWP' : YTD_DWP
                }
            ]
        }
        # Post the record in Specialty LOB
        lob_response = requests.post(
            url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
        if(lob_response.json()['data'][0]['status'] == "error"):
            print(lob_response.json())
            print(request_body)
            return


           # You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))

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
