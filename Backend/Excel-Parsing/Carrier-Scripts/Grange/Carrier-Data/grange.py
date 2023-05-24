from cmath import nan
from email import header

from numpy import NaN, rec
import pandas as pd
from difflib import SequenceMatcher
import requests
import json




client_id = '1000.WI69YDZMVKOI2OVL22NID700CTMNTD'
client_secret = '9689b45819ebb9b16ffab2d96ae9108ebefee9eae5'
refresh_token = '1000.92c7d1df11e9e5b8d259c7edf73fdb88.e918f39048f8f838075705efcd73c28c'



auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret




consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'


def main():

  
    auth_response = requests.post(url=auth_url)

    # print(auth_response.json())

    # turn the response into readable json and store the access token in a variable
    access_token = auth_response.json()['access_token']


    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    # read carrier and zoho data in using pandas

    grange_data_sheet_1 = pd.read_excel('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Grange\\Carrier Data\\Grange - Agency Collective - Nov 2021.xlsx', sheet_name='AGENCY COLLECTIVE', header=3)
    grange_data_sheet_2 = pd.read_excel('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Grange\\Carrier Data\\Grange - Agency Collective - Nov 2021.xlsx', 'AGENCY COLLECTIVE SUMMARY')

    zoho_account_data = pd.read_csv('C:\\Users\\rtran\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Accounts_001.csv')
    zoho_agency_code_data = pd.read_csv('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Zoho-Data\\Agency_Codes_C_001.csv')

    # create id dictionaries (maps)

    account_id_map = {}
    agency_code_id_map = {}


    # loop through zoho agency code data
    for i, row in zoho_agency_code_data.iterrows():

        # you only want agency codes from the carrier you are working with, so replace INSERT_CARRIER_NAME with the correct name

        if(row["Carrier"] == "Grange"):

    
            account_id = str(row["Agency ID"]).split("_")[1]

            # strip the agency code of any whitespace
            agency_code = str(row["Agency Code"]).strip()

            # this creates a new key in the agency code id map and stores the account id, agency code id, and contact id

            # if you were to print this specific key:value out out, it would look like this
            # {
            # note that these numbers are just for example
            #   '4893029': {
            #       'acc_id': 5784930293487508430,
            #       'agency_code_id': 2343289790923879
            #       'cont_id': 58490303948500948
            #   }
            #
            # }

            #
            agency_code_id_map[agency_code] = {
                "acc_id": account_id,
                "agency_code_id": str(row["Record Id"]).split("_")[1],
            }
            # not every agency code has a contact id associated with it, so we have
            # to use a try except so our code doesn't break when it tries to perform this string manipulation. The error is an IndexError
            # It would be to your benefit seeing the error, so i highly recommend taking this part code out of the
            # try except statement and trying to run it
            try:
                agency_code_id_map[agency_code]['contact_id'] = str(
                    row["Agency Contact ID"]).split("_")[1]

            except IndexError:
                agency_code_id_map[agency_code]['contact_id'] = ""

    # print(account_id_map)

    for i, row in zoho_account_data.iterrows():
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

    # print(agency_code_id_map)

    carrier_report_id = 5187612000001241005


    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    lob_member_data = []

    print(grange_data_sheet_1)
    grange_data_sheet_2 = grange_data_sheet_2.dropna()
    print(grange_data_sheet_2)

    # # Consolidated Line Total
    # YTD_DWP = round(grange_data_sheet_1.iloc[0]["YTD DWP"])
    # Prior_YTD_DWP = round(grange_data_sheet_1.iloc[0]["Prior YTD DWP"])
    # DWP_12MM = round(grange_data_sheet_1.iloc[0]["Rolling 12 DWP"])

    # request_body = {
    #             "data": [
    #                 {
    #                     "Name": name_string,
    #                     "Carrier_Report": carrier_report_id,
    #                     "Account": account_id,
    #                     "Agency_Code": agency_code_id,
    #                     "Contact": contact_id,
    #                     'YTD_DWP': YTD_DWP,
    #                     'Prior_YTD_DWP': Prior_YTD_DWP,
    #                     'DWP_12MM': DWP_12MM
    #                     # add additional fields here
    #                 } 
    #             ]
    #         }


    for i, row in grange_data_sheet_1.iloc[2:].iterrows():
        print()
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        agency_name = str(row['Agency Name'])
        agency_code = str(row["Agency Number"])

        name_string = agency_name + " - Grange - November 2021"
        agency_code = agency_code[:-2]
        # print(agency_name)
        # print(agency_code)

#         # this dictionary will hold our API request data when we're ready to send it
#         request_body = {}

#         # these are the variables that store the value of the corresponding cell.
        YTD_DWP = row["YTD DWP"]
        Prior_YTD_DWP = row["Prior YTD DWP"]
        DWP_12MM = row["Rolling 12 DWP"]
        Month_DWP = row["Month DWP"]
        # Prior_12MM_DWP = 
        # YTD_DWP_Growth =
        # Earned_Premium_12MM = 
        # Primary_Contact =
        YTD_NB_DWP = row["YTD NB"]
        Prior_YTD_NB_DWP = row["Prior YTD NB"]
        NB_DWP_12MM = row["Rolling 12 NB"]
        # YTD_NB_DWP_Growth =
        YTD_PIF = row["YTD PIF"]
        Prior_YTD_PIF = row["Prior YTD PIF"]
        # PIF_12MM =
        # YTD_PIF_Growth =
        # Incurred_Loss_12MM =
        # Capincurred_Loss_12MM =
        Loss_Ratio_12MM = row["Rolling 12 LR"]
        YTD_Loss_Ratio = row["YTD LR"]
        YTD_Quotes = row["YTD Quotes"]
        Prior_YTD_Quotes = row["Quote Count - Prior YTD"]
        Quotes_12MM = row["Rolling 12 Quotes"]
        # YTD_Quote_Growth = 
        # print(YTD_DWP)


#         try:

#             # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
#             account_id = agency_code_id_map[agency_code]["acc_id"]
#             agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
#             contact_id = agency_code_id_map[agency_code]["contact_id"]

#             # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.


#             # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names

#             request_body = {
#                 "data": [
#                     {
#                         "Name": name_string,
#                         "Carrier_Report": carrier_report_id,
#                         "Account": account_id,
#                         "Agency_Code": agency_code_id,
#                         "Contact": contact_id,
#                         'YTD_DWP': YTD_DWP,
#                         'Prior_YTD_DWP': Prior_YTD_DWP,
#                         'DWP_12MM': DWP_12MM
#                         # add additional fields here
#                     } 
#                 ]
#             }   

#             # this is our API call to the CRM API to post a record in the Member Consolidated Module.

#             consolidated_response = requests.post(
#                 url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

#             # if there is an error, the code will end and it prints the request body and the response

#             if(consolidated_response.json()['data'][0]['status'] == "error"):
#                 print("  ")
#                 print(request_body)
#                 print(consolidated_response.json())
#                 print("  ")
#                 return
#             else:
#                 # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
#                 member_consolidated_id = consolidated_response.json()[
#                     'data'][0]['details']['id']
#                 member_consolidated_id += 1

#             # Consolidated Lines to upload under Agency
#             for i, row in grange_data_sheet_2.iloc[1:].iterrows():
#                 count = 0
#                 # Personal Line
#                 if row['Unnamed: 0'] == agency_name and count == 0:
#                     YTD_DWP = row["Unnamed: 10"]
#                     Prior_YTD_DWP = row["Unnamed: 11"]
#                     DWP_12MM = row["Unnamed: 13"]
#                     count += 1
#                 # Commercial Line
#                 elif row['Unnamed: 0'] == agency_name and count == 1:
#                     YTD_DWP = row["Unnamed: 10"]
#                     Prior_YTD_DWP = row["Unnamed: 11"]
#                     DWP_12MM = row["Unnamed: 13"]
#                     count += 1
#                 # Speciality Line
#                 elif row['Unnamed: 0'] == agency_name and count == 2:
#                     YTD_DWP = row["Unnamed: 10"]
#                     Prior_YTD_DWP = row["Unnamed: 11"]
#                     DWP_12MM = row["Unnamed: 13"]
#                     count += 1
                


#         except KeyError:
#             # This exception part is only triggered by a keyError. The KeyError means no agency code was found in our system data (agency code map) for the given row agency code. This means we need to create a new agency code record in the system for this row in the carrier data, and link it to the correct account and contact.

#             # two things can happen here when you're looking at a new carrier dataset: It has a UAC number column, and it doesn't.

#             # if they have a UAC number column, you MUST insert another Try-Except statement INSIDE this KeyError exception. This try except statement tries to map the UAC number to the zoho agency code data. If a match using the UAC number is found in the zoho agency code data, then a new agency code is created with that UAC number as a new agency code and the relevant associated data.

#             # if they do not have a UAC number column, then you DO NOT NEED another try-except statement.

#             # This template assumes that there IS NOT a UAC column. If you run into a dataset with a UAC column, look at nationwide.py. It's not commented, so feel free to ask me if you need help.
#             # Since no agency code match was found, we are going to have to try and find an account and contact id to match to the new agency code record we are going to create. We'll loop through the account id map and see if the given agency name on this row of carrier data is in they key we are looping through and vice versa.

#             # before we put it through the loop, we'll use our horrific piece of string concatenation to try and lower our missing matches. If you don't think this is enough, use the similar function written below the main function to find a good ratio.

#             formatted_agency_name = agency_name.strip(
#             ).lower().replace(',', "").replace('.', "")

#             for key in account_id_map:
#                 if(formatted_agency_name in key):
#                     account_id = account_id_map[key]['acc_id']
#                     contact_id = account_id_map[key]['cont_id']
#                 if(key in formatted_agency_name):
#                     account_id = account_id_map[key]['acc_id']
#                     contact_id = account_id_map[key]['cont_id']

        

#             # When our code gets here, we either have an account matched or we don't. If we do, we proceed normally and make an agency code record and member consolidated record with the correct account and contact id. If not, we have an account for unassigned records named, you guessed it: 'Unassigned.'

#             if(account_id == ''):
#                 # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.
                
#                 account_id = 5187612000000754742

#                 request_body = {
#                     "data": [
#                         {
#                             "Account": account_id,
#                             "Name": agency_code,
#                             "UAC": uac_number,
#                             "Carrier": "Grange",
#                             "UAC_Name": agency_name,
#                             "Contact": contact_id
#                             # add additional fields here
#                         }
#                     ]
#                 }
#                 # this is our API call to the CRM API to post a record in the Agency Code Module.

#                 agency_code_response = requests.post(
#                     url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

#                 # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.

#                 if(agency_code.json()['data'][0]['status'] == "error"):

#                     print("  ")
#                     print(request_body)
#                     print(agency_code_response.json())
#                     print("  ")
#                     return
#                 else:
#                     # if it returns a successful response, then the created agency code id is stored into the corresponding variable

#                     agency_code_id = agency_code_response.json()[
#                         'data'][0]['details']['id']
#                     new_agency_code_count += 1
#                     request_body = {
#                         "data": [
#                             {
#                                 "Name": name_string,
#                                 "Carrier_Report": carrier_report_id,
#                                 "Account": account_id,
#                                 "Agency_Code": agency_code_id,
#                                 "Contact": contact_id,
#                                 'YTD_DWP': YTD_DWP,
#                                 'Prior_YTD_DWP': Prior_YTD_DWP,
#                                 'DWP_12MM': DWP_12MM
#                                 # add additional fields here

#                             }
#                         ]
#                     }
#                     consolidated_response = requests.post(
#                         url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

#                     if(consolidated_response.json()['data'][0]['status'] == "error"):

#                         print("  ")
#                         print(request_body)
#                         print(consolidated_response.json())
#                         print("  ")
#                         return
#                     else:
#                         # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
#                         member_consolidated_id = consolidated_response.json()[
#                             'data'][0]['details']['id']
#                         member_consolidated_id += 1

#             else:
#                 # we found an account id match so now we just need to create our agency code record.

#                 request_body = {
#                     "data": [
#                         {
#                             "Account": account_id,
#                             "Name": agency_code,
#                             "UAC": uac_number,
#                             "Carrier": "Grange",
#                             "UAC_Name": agency_name,
#                             "Contact": contact_id
#                             # add additional fields here
#                         }
#                     ]
#                 }
#                 # this is our API call to the CRM API to post a record in the Agency Code Module.

#                 agency_code_response = requests.post(
#                     url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

#                 # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.

#                 if(agency_code.json()['data'][0]['status'] == "error"):

#                     print("  ")
#                     print(request_body)
#                     print(agency_code_response.json())
#                     print("  ")
#                     return
#                 else:
#                     # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

#                     agency_code_id = consolidated_response.json()[
#                         'data'][0]['details']['id']
#                     new_agency_code_count += 1
#                     request_body = {
#                         "data": [
#                             {
#                                 "Name": name_string,
#                                 "Carrier_Report": carrier_report_id,
#                                 "Account": account_id,
#                                 "Agency_Code": agency_code_id,
#                                 "Contact": contact_id,
#                                 'YTD_DWP': YTD_DWP,
#                                 'Prior_YTD_DWP': Prior_YTD_DWP,
#                                 'DWP_12MM': DWP_12MM
#                                 # add additional fields here

#                             }
#                         ]
#                     }

#                 # now we make our member consolidated record.

#                 # this is our API call to the CRM API to post a record in the Member Consolidated Module.

#                     consolidated_response = requests.post(
#                         url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

#                     if(consolidated_response.json()['data'][0]['status'] == "error"):

#                         print("  ")
#                         print(request_body)
#                         print(consolidated_response.json())
#                         print("  ")
#                         return
#                     else:
#                         # if it returns a successful response, then the created member consolidated record id is stored into the corresponding variable
#                         member_consolidated_id = consolidated_response.json()[
#                             'data'][0]['details']['id']
#                         member_consolidated_id += 1

#         # You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.


#     print('Consolidated records created: '+ str(member_consolidated_count))
#     print('Agency Codes created: '+ str(new_agency_code_count))
#     return






# def similar(a, b):
#     return SequenceMatcher(None, a, b).ratio()





main()
