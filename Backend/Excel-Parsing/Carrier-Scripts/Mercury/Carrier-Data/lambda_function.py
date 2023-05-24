import pandas as pd
import requests
import json
import boto3
import io


def lambda_handler(event, context):
    key = event['params']['querystring']['key']
    filename = event['params']['querystring']['filename']
    dot = filename.index(".")
    filename = filename[:dot]
    response =  {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*"
        },
        "body": json.dumps({
            'status':'successful'
        })
    }
    count = 0
    
    client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
    client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
    refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'
    
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

    # read carrier data in using pandas

    carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl')


    account_id_map = {}
    agency_code_id_map = {}

    # This is the search function to find all existing account ids and adds them to a account id map
    coql_url = 'https://www.zohoapis.com/crm/v2/coql'
    more_records = True
    query_offset = 0
    agency_id_records = []
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Account_Name, Primary_Contact from Accounts where Account_Name != '' limit " + str(query_offset) + ", 100"

                                        }))
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        agency_id_records.append(coql_response.json()['data'])
        query_offset += 100


    # loop through each list in the hash map 
    count = 0
    for i in range(len(agency_id_records)):
        for x in range(len(agency_id_records[i])):
            current_record = (agency_id_records[i][x])
            count += 1
            account_name = current_record['Account_Name'].strip(
            ).lower().replace(',', "").replace('.', "")

            account_id_map[account_name] = {
                'acc_id': str(current_record['id']),
              }
        try:
            [account_name]['cont_id'] = str(current_record['Primary_Contact']['id'])

        except TypeError:
                account_id_map[account_name]['cont_id'] = ''

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0
    lob_member_data = []


    # This is the search function used to find all existing agency codes and adds them to the agency code map
    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Mercury',
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
                
    # Create Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    word_date = list_words[2]
    space = list_words[2].index(" ")
    month = list_words[2][:3]
    year = list_words[2][space:]
    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    # Create Carrier Report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Name = 'Mercurytest - ' + word_date
    carrier_data = carrier_data.fillna(0)
    YTD_DWP = carrier_data["Written Premium"].iloc[1]
    YTD_Quotes = carrier_data["Quotes"].iloc[-1]
    YTD_PIF = carrier_data["PIF"].iloc[-1]
    YTD_Hit_Ratio = carrier_data["Close Rate"].iloc[-1]
    YTD_Incurred_Losses = carrier_data["Avg Prem /Loss"].iloc[-1]
    YTD_Loss_Ratio = carrier_data["Loss Ratio (CY)"].iloc[-1]
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Mercury Insurance",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_DWP" : YTD_DWP,
                    "YTD_Quotes" : YTD_Quotes,
                    "YTD_PIF" : YTD_PIF,
                    "YTD_Hit_Ratio" : YTD_Hit_Ratio,
                    "YTD_Incurred_Losses" : YTD_Incurred_Losses,
                    "YTD_Loss_Ratio" : YTD_Loss_Ratio
            }]
        }
    carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = (carrier_report.json())
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])


    for i, row in carrier_data[0:-2].iterrows():
        if row["Child Agency"] != "Total":
            # loop variables. These are reset every new row and must be filled in.
            # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            uac_number = ''  # if needed
            member_consolidated_id = ''
            agency_code = row['Child Agency']
            colon = agency_code.index(":")
            agency_name = agency_code[colon+2:]
            agency_code = agency_code[:colon]
            name_string = agency_name + " - Mercury - " + word_date
            YTD_DWP = row["Written Premium"]
            YTD_Quotes = row["Quotes"]
            YTD_PIF = row["PIF"]
            YTD_Hit_Ratio = row["Close Rate"]
            YTD_Incurred_Losses = row["Avg Prem /Loss"]
            YTD_Loss_Ratio = format_number(row["Loss Ratio (CY)"])


            # this dictionary will hold our API request data when we're ready to send it
            request_body = {}

            # these are the variables that store the value of the corresponding cell.


            try:
                # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables

                account_id = agency_code_id_map[agency_code]["acc_id"]
                contact_id = agency_code_id_map[agency_code]["contact_id"]
                agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]

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
                    # agency_code_id = account_id_map[formatted_agency_name]['agency_code_id']
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
                        "Carrier": "Mercury",
                        "UAC_Name": agency_name,
                        "Contact": contact_id
                        # add additional fields here
                    }]
                }
                # this is our API call to the CRM API to post a record in the Agency Code Module.

                agency_code_response = requests.post(
                    url=agency_code_url,
                    headers=headers,
                    data=json.dumps(request_body).encode('utf-8'))

                # note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.

                if (agency_code_response.json()['data'][0]['status'] == "error"):
                    print("  ")
                    print(request_body)
                    print(agency_code_response.json())
                    print("  ")
                    return

                agency_code_id = agency_code_response.json()['data'][0]['details']['id']
                    # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

            request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        "Carrier": "Mercury Insurance",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "YTD_DWP" : YTD_DWP,
                        "YTD_Quotes" : YTD_Quotes,
                        "YTD_PIF" : YTD_PIF,
                        "YTD_Hit_Ratio" : YTD_Hit_Ratio,
                        "YTD_Incurred_Losses" : YTD_Incurred_Losses,
                        "YTD_Loss_Ratio" : YTD_Loss_Ratio
                }]
            }

            # now we make our member consolidated record.

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
    