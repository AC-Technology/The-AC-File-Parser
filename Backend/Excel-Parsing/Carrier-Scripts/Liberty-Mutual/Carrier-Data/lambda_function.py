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

    carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl',sheet_name='Sub Agency')


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
    carrier_data.columns = carrier_data.loc[14]
    

    # This is the search function used to find all existing agency codes and adds them to the agency code map
    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Liberty Mutual',
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


    #Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    word_date = list_words[3]
    space = list_words[3].index(" ")
    month = list_words[3][:3]
    year = list_words[3][space:]
    day = days[month]
    month = months[month]


    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    # Create Carrier Report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Name = 'Liberty Mutual - ' + word_date
    
    DWP_12MM = carrier_data["CY R12 DWP"].iloc[-1]

    YTD_DWP = carrier_data["CY YTD DWP"].iloc[-1]

    Prior_YTD_DWP = carrier_data['PY YTD DWP'].iloc[-1]

    YTD_DWP_Growth = format_number(carrier_data["YTD DWP Growth"].iloc[-1])

    YTD_NB_DWP = carrier_data["CY YTD NB"].iloc[-1]

    Prior_YTD_NB_DWP = carrier_data["PY YTD NB"].iloc[-1]

    YTD_NB_DWP_Growth = format_number(carrier_data["YTD NB Growth %"].iloc[-1])

    NB_DWP_12MM = carrier_data["CY R12 NB"].iloc[-1]

    YTD_Quotes = carrier_data["CY YTD Quotes"].iloc[-1]

    Prior_YTD_Quotes = carrier_data["PY YTD Quotes"].iloc[-1]

    YTD_Quote_Growth = format_number(carrier_data["YTD Quote Growth %"].iloc[-1])

    YTD_Hit_Ratio = format_number(carrier_data["CY Hit Ratio %"].iloc[-1])

    Prior_YTD_Hit_Ratio = format_number(carrier_data["PY Hit Ratio %"].iloc[-1])

    YTD_PIF = carrier_data["PLIF YTD"].iloc[-1]

    Earned_Premium_YTD = carrier_data["CY YTD EP"].iloc[-1]
        
    YTD_Loss_Ratio = format_number(carrier_data["CAL YTD Loss Ratio %"].iloc[-1])

    YTD_Quote_Growth = format_number(carrier_data["YTD Quote Growth %"].iloc[-1])

    request_body = {
                "data": [{
                    "Name": Name,
                    "Carrier": "Liberty Mutual",
                    "Report_Date": Report_Date,
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
                }]
            }   
    carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = (carrier_report.json())
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])
    
    count = 0
    for i, row in carrier_data[15:-1].iterrows():
        # check if it is not a duplicate
        if row["CI Business Unit"] == "BI":
            count += 1
            print(count)

            # Reset variable
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            uac_number = ''  
            member_consolidated_id = ''
            # this dictionary will hold our API request data when we're ready to send it
            request_body = {}

            agency_name = str(row['Agency Name']).strip()

            agency_code = str(row["Sub Agency Code"]).strip()

            name_string = agency_name + " - Liberty Mutual - " + word_date

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
                    "Report_Date": Report_Date,
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
