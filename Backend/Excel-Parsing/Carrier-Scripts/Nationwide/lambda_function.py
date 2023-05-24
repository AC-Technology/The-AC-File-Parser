import pandas as pd
import requests
import json
import io
import sys
import boto3

s3 = boto3.client('s3')

def main():
    try:
        # Receive parameters from command line
        key = sys.argv[5]
        filename = sys.argv[1] + " " + sys.argv[2] + " " + sys.argv[3] + " "  + sys.argv[4]
        # key = "5343391.xlsx"
        # filename = "Nationwide - January 2022.xlsx"
        dot = filename.index(".")
        filename = filename[:dot]

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


        # This is the search function used to find all existing agency codes and adds them to the agency code map
        more_records = True
        agency_code_records =[]
        page = 1
        while more_records:
            search_params = {'criteria': 'Carrier:equals:Nationwide',
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
                    
        member_consolidated_count = 0
        new_agency_code_count = 0

        count = 0

        # read carrier data in using pandas
        try:
            carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl', sheet_name = "The Agency Cluster")
        except:
            carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl', sheet_name = "The Agency Collective")

        carrier_data = carrier_data.fillna(0)
        carrier_data = carrier_data.replace(' ', 0)
        carrier_data = carrier_data.replace('-', 0)

        carrier_data.columns = carrier_data.iloc[1]

        # Create the Report Date
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

        carrier_data = carrier_data.fillna("")
        # Create the Carrier Report
        carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
        Name = 'Nationwide - ' + word_date

        if month == 1:
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP"].iloc[0])
            Prior_YTD_NB_DWP = int(carrier_data["Prior YTD NB DWP"].iloc[0])
            YTD_NB_DWP = int(carrier_data["Curr. YTD NB DWP"].iloc[0])
            NB_DWP_12MM = int(carrier_data['New DWP 12MM'].iloc[0])
            PIF_NB_12MM = int(carrier_data["New PIF 12MM"].iloc[0])
            YTD_PIF = int(carrier_data["PIF YTD"].iloc[0])
            Incurred_Loss_12MM = int(carrier_data["Incurred Loss 12MM"].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM'].iloc[0])
            Earned_Premium_12MM = int(carrier_data["Earned Premium 12MM"].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data["12MM Loss Ratio %"].iloc[0])
            Prior_YTD_Quotes = carrier_data["Prior YTD Quotes"].iloc[0]
            YTD_Quotes = carrier_data["YTD Quotes"].iloc[0]
            Quotes_12MM = carrier_data["12MM Quotes"].iloc[0]
            req_body = {
                    "data": [
                        {
                            "Name": Name,
                            'Carrier':'Nationwide',
                            'Report_Type': ['Year End'],
                            'Report_Date': Report_Date,
                            "YTD_DWP": YTD_DWP,
                            "Prior_YTD_DWP": Prior_YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            "YTD_DWP_Growth": YTD_DWP_Growth,
                            "Earned_Premium_12MM": Earned_Premium_12MM,
                            'YTD_NB_DWP': YTD_NB_DWP,
                            'Prior_YTD_New_DWP':  Prior_YTD_NB_DWP,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'YTD_PIF': YTD_PIF,
                            "NB_PIF_12MM": PIF_NB_12MM,
                            "Incurred_Loss_12MM": Incurred_Loss_12MM,
                            "Capincurred_Loss_12MM": Capincurred_Loss_12MM,
                            "Loss_Ratio_12MM": Loss_Ratio_12MM,
                            "YTD_Quotes": YTD_Quotes,
                            "Prior_YTD_Quotes": Prior_YTD_Quotes,
                            "Quotes_12MM":Quotes_12MM
                        }
                    ]
                }
            carrier_report = requests.post(
                        url=carrier_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            carrier_report_id = (carrier_report.json())
            carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

            # Create LOB Report
            # PL Section
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP (PL)"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP (PL)"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth (PL)"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP (PL)"].iloc[0])
            YTD_NB_PIF = int(carrier_data['New PIF YTD (PL)'].iloc[0])
            NB_DWP_12MM = int(carrier_data['12MM New DWP (PL)'].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM (PL)'].iloc[0])
            Earned_Premium_12MM = int(carrier_data['Earned Premium 12MM (PL)'].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data['12MM LR (PL)'].iloc[0])
            Line_of_Business = 'Personal'

            lob_name_string = Name + " - " + Line_of_Business
            lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'

            req_body = {
                'data': [
                    {
                        'Name': lob_name_string,
                        'Report_Type': ['Year End'],
                        'Report_Date': Report_Date,
                        'Carrier_Report': carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }
            ac_lob = requests.post(
                    url= lob_total_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            ac_lob_total_id = (ac_lob.json())
            personal_id = (ac_lob_total_id["data"][0]["details"]['id'])

            # CL Section
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP (CL)"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP (CL)"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth (CL)"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP (CL)"].iloc[0])
            YTD_NB_PIF = int(carrier_data['New PIF YTD (CL)'].iloc[0])
            NB_DWP_12MM = int(carrier_data['12MM New DWP (CL)'].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM (CL)'].iloc[0])
            Earned_Premium_12MM = int(carrier_data['Earned Premium 12MM (CL)'].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data['12MM LR (CL)'].iloc[0])
            Line_of_Business = 'Commercial'

            lob_name_string = Name + " - " + Line_of_Business

            req_body = {
                'data': [
                    {
                        'Name': lob_name_string,
                        'Report_Type': ['Year End'],
                        'Report_Date': Report_Date,
                        'Carrier_Report': carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }
            ac_lob = requests.post(
                    url= lob_total_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            ac_lob_total_id = (ac_lob.json())
            commercial_id = (ac_lob_total_id["data"][0]["details"]['id'])

            for i, row in carrier_data.iloc[2:].iterrows():
                # first create member consolidated
                # then add member lob with consolidated id and lob total id
                count += 1
                print(count)

                agency_code = str(row["Agency Code"]).strip()
                if(len(agency_code) == 5):
                    new_code = "000" + agency_code
                    agency_code = new_code
                req_body = {}
                # uac_number = str(row["UAC"]).strip().split(":")[0]
                # uac_name = str(row["UAC"]).strip().split(":")[1]
                uac_number = str(row["UAC"]).strip()
                uac_name = str(row["UAC Name"]).strip()
                if(len(uac_number) == 5):
                    new_uac = "000" + uac_number
                    uac_number = new_uac
                account_id = ''
                contact_id = ''
                agency_code_id = ''
                member_consolidated_id = ''

                name_string = row["Agency Name"] + " - Nationwide - " + word_date
                agency_name = str(row['Agency Name']).strip(
                ).lower().replace(',', "").replace('.', "")
                Prior_YTD_DWP = int(row["Prior YTD DWP"])
                YTD_DWP = int(row["Curr. YTD DWP"])
                YTD_DWP_Growth = format_percentage(row["YTD DWP Growth"])
                DWP_12MM = int(row["12MM DWP"])
                Prior_YTD_NB_DWP = int(row["Prior YTD DWP"])
                YTD_NB_DWP = int(row["Curr. YTD NB DWP"])
                NB_DWP_12MM = int(row['New DWP 12MM'])
                PIF_NB_12MM = int(row["New PIF 12MM"])
                YTD_PIF = int(row["PIF YTD"])
                Incurred_Loss_12MM = int(row["Incurred Loss 12MM"])
                Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM'])
                Earned_Premium_12MM = int(row["Earned Premium 12MM"])
                Loss_Ratio_12MM = format_percentage(row["12MM Loss Ratio %"])
                Prior_YTD_Quotes = row["Prior YTD Quotes"]
                YTD_Quotes = row["YTD Quotes"]
                Quotes_12MM = row["12MM Quotes"]

                try:
                    
                    # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
                    account_id = agency_code_id_map[agency_code]["acc_id"]
                    agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
                    contact_id = agency_code_id_map[agency_code]["contact_id"]
                    # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.
                    # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names
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
                                "Carrier": "Nationwide",
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

                req_body = {
                    "data": [
                        {
                            "Name": name_string,
                            "Carrier_Report": carrier_report_id,
                            "Account": account_id,
                            "Agency_Code": agency_code_id,
                            "Contact": contact_id,
                            'Carrier':'Nationwide',
                            "Uniform_Agency_Code": uac_number,
                            'Report_Type': ['Year End'],
                            'Report_Date': Report_Date,
                            "YTD_DWP": YTD_DWP,
                            "Prior_YTD_DWP": Prior_YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            "YTD_DWP_Growth": YTD_DWP_Growth,
                            "Earned_Premium_12MM": Earned_Premium_12MM,
                            'YTD_NB_DWP': YTD_NB_DWP,
                            'Prior_YTD_NB_DWP':  Prior_YTD_NB_DWP,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'YTD_PIF': YTD_PIF,
                            "PIF_NB_12MM": PIF_NB_12MM,
                            "Incurred_Loss_12MM": Incurred_Loss_12MM,
                            "Capincurred_Loss_12MM":   Capincurred_Loss_12MM,
                            "Loss_Ratio_12MM": Loss_Ratio_12MM,
                            "YTD_Quotes": YTD_Quotes,
                            "Prior_YTD_Quotes": Prior_YTD_Quotes,
                            "Quotes_12MM": Quotes_12MM
                        }
                    ]

                }

                consolidated_res = requests.post(
                    url=consolidated_url, headers=headers, data=json.dumps(req_body))
                if(consolidated_res.json()['data'][0]['status'] == "error"):
                    print(req_body)
                    print(consolidated_res.json())
                    return
                member_consolidated_id = consolidated_res.json()[
                    'data'][0]['details']['id']
                member_consolidated_count += 1

                # PL Section
                Prior_YTD_DWP = int(row["Prior YTD DWP (PL)"])
                YTD_DWP = int(row["Curr. YTD DWP (PL)"])
                YTD_DWP_Growth = format_percentage(row["YTD DWP Growth (PL)"])
                DWP_12MM = int(row["12MM DWP (PL)"])
                YTD_NB_PIF = int(row['New PIF YTD (PL)'])
                NB_DWP_12MM = int(row['12MM New DWP (PL)'])
                Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM (PL)'])
                Earned_Premium_12MM = int(row['Earned Premium 12MM (PL)'])
                Loss_Ratio_12MM = format_percentage(row['12MM LR (PL)'])
                Line_of_Business = 'Personal'

                lob_name_string = row["Agency Name"] + \
                    " - Nationwide - January 2022 - " + Line_of_Business

                req_body = {
                    'data': [
                        {
                            'Account': account_id,
                            'Contact': contact_id,
                            'Agency_Code': agency_code_id,
                            'Consolidated_Record': member_consolidated_id,
                            'AC_Total': personal_id,
                            'Name': lob_name_string,
                            'Report_Type': ['Year End'],
                            'Report_Date': Report_Date,
                            'Line_of_Business': Line_of_Business,
                            'Carrier': 'Nationwide',
                            'Uniform_Agency_Code':uac_number,
                            'Name': lob_name_string,
                            'Prior_YTD_DWP': Prior_YTD_DWP,
                            'YTD_DWP': YTD_DWP,
                            'YTD_DWP_Growth':YTD_DWP_Growth,
                            'DWP_12MM': DWP_12MM,
                            'YTD_NB_PIF': YTD_NB_PIF,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                            'Earned_Premium_12MM': Earned_Premium_12MM,
                            'Loss_Ratio_12MM': Loss_Ratio_12MM

                        }
                    ]
                }
                lob_response = requests.post(
                    url=member_lob_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
                if(lob_response.json()['data'][0]['status'] == "error"):
                    print(lob_response.json())
                    print(req_body)
                    return

                # CL Section
                Line_of_Business = 'Commercial'
                lob_name_string = row["Agency Name"] + \
                    " - Nationwide - January 2022 - " + Line_of_Business
                Prior_YTD_DWP = int(row["Prior YTD DWP (CL)"])
                YTD_DWP = int(row["Curr. YTD DWP (CL)"])
                YTD_DWP_Growth = format_percentage(row["YTD DWP Growth (CL)"])
                DWP_12MM = int(row["12MM DWP (CL)"])
                YTD_NB_PIF = int(row['New PIF YTD (CL)'])
                NB_DWP_12MM = int(row['12MM New DWP (CL)'])
                Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM (CL)'])
                Earned_Premium_12MM = int(row['Earned Premium 12MM (CL)'])
                Loss_Ratio_12MM = format_percentage(row['12MM LR (CL)'])
                req_body = {
                    'data': [
                        {
                            'Account': account_id,
                            'Contact': contact_id,
                            'Agency_Code': agency_code_id,
                            'Consolidated_Record': member_consolidated_id,
                            'AC_Total': commercial_id,
                            'Name': lob_name_string,
                            'Report_Type': ['Year End'],
                            'Report_Date': Report_Date,
                            'Line_of_Business': Line_of_Business,
                            'Carrier': 'Nationwide',
                            'Uniform_Agency_Code':uac_number,
                            'Name': lob_name_string,
                            'Prior_YTD_DWP': Prior_YTD_DWP,
                            'YTD_DWP': YTD_DWP,
                            'YTD_DWP_Growth':YTD_DWP_Growth,
                            'DWP_12MM': DWP_12MM,
                            'YTD_NB_PIF': YTD_NB_PIF,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                            'Earned_Premium_12MM': Earned_Premium_12MM,
                            'Loss_Ratio_12MM': Loss_Ratio_12MM

                        }
                    ]
                }

                lob_response = requests.post(
                    url=member_lob_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
                if(lob_response.json()['data'][0]['status'] == "error"):
                    print(lob_response.json())
                    print(req_body)
                    return
        else:
            #Carrier report
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP"].iloc[0])
            Prior_YTD_NB_DWP = int(carrier_data["Prior YTD NB DWP"].iloc[0])
            YTD_NB_DWP = int(carrier_data["Curr. YTD NB DWP"].iloc[0])
            NB_DWP_12MM = int(carrier_data['New DWP 12MM'].iloc[0])
            PIF_NB_12MM = int(carrier_data["New PIF 12MM"].iloc[0])
            YTD_PIF = int(carrier_data["PIF YTD"].iloc[0])
            Incurred_Loss_12MM = int(carrier_data["Incurred Loss 12MM"].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM'].iloc[0])
            Earned_Premium_12MM = int(carrier_data["Earned Premium 12MM"].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data["12MM Loss Ratio %"].iloc[0])
            req_body = {
                    "data": [
                        {
                            "Name": Name,
                            'Carrier':'Nationwide',
                            'Report_Type': ['YTD'],
                            'Report_Date': Report_Date,
                            "YTD_DWP": YTD_DWP,
                            "Prior_YTD_DWP": Prior_YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            "YTD_DWP_Growth": YTD_DWP_Growth,
                            "Earned_Premium_12MM": Earned_Premium_12MM,
                            'YTD_NB_DWP': YTD_NB_DWP,
                            'Prior_YTD_New_DWP':  Prior_YTD_NB_DWP,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'YTD_PIF': YTD_PIF,
                            "NB_PIF_12MM": PIF_NB_12MM,
                            "Incurred_Loss_12MM": Incurred_Loss_12MM,
                            "Capincurred_Loss_12MM": Capincurred_Loss_12MM,
                            "Loss_Ratio_12MM": Loss_Ratio_12MM,
                        }
                    ]
                }
            carrier_report = requests.post(
                        url=carrier_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            carrier_report_id = (carrier_report.json())
            carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

            lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
            # Create LOB Report
            # PL Section
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP (PL)"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP (PL)"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth (PL)"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP (PL)"].iloc[0])
            YTD_NB_PIF = int(carrier_data['New PIF YTD (PL)'].iloc[0])
            NB_DWP_12MM = int(carrier_data['12MM New DWP (PL)'].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM (PL)'].iloc[0])
            Earned_Premium_12MM = int(carrier_data['Earned Premium 12MM (PL)'].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data['12MM LR (PL)'].iloc[0])
            Line_of_Business = 'Personal'

            lob_name_string = Name + " - " + Line_of_Business

            req_body = {
                'data': [
                    {
                        'Name': lob_name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'Carrier_Report': carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }
            ac_lob = requests.post(
                    url= lob_total_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            ac_lob_total_id = (ac_lob.json())
            personal_id = (ac_lob_total_id["data"][0]["details"]['id'])

            # CL Section
            Prior_YTD_DWP = int(carrier_data["Prior YTD DWP (CL/Farm)"].iloc[0])
            YTD_DWP = int(carrier_data["Curr. YTD DWP (CL/Farm)"].iloc[0])
            YTD_DWP_Growth = format_percentage(carrier_data["YTD DWP Growth (CL/Farm)"].iloc[0])
            DWP_12MM = int(carrier_data["12MM DWP (CL/Farm)"].iloc[0])
            YTD_NB_PIF = int(carrier_data['New PIF YTD (CL/Farm)'].iloc[0])
            NB_DWP_12MM = int(carrier_data['12MM New DWP (CL/Farm)'].iloc[0])
            Capincurred_Loss_12MM = int(carrier_data['CapIncurred Loss 12MM (CL/Farm)'].iloc[0])
            Earned_Premium_12MM = int(carrier_data['Earned Premium 12MM (CL/Farm)'].iloc[0])
            Loss_Ratio_12MM = format_percentage(carrier_data['12MM LR (CL/Farm)'].iloc[0])
            Line_of_Business = 'Commercial'

            lob_name_string = Name + " - " + Line_of_Business

            req_body = {
                'data': [
                    {
                        'Name': lob_name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'Carrier_Report': carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }
            ac_lob = requests.post(
                    url= lob_total_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            ac_lob_total_id = (ac_lob.json())
            commercial_id = (ac_lob_total_id["data"][0]["details"]['id'])

        for i, row in carrier_data.iloc[2:].iterrows():
            # first create member consolidated
            # then add member lob with consolidated id and lob total id
            count += 1
            print(count)

            agency_code = str(row["Agency Code"]).strip()
            if(len(agency_code) == 5):
                new_code = "000" + agency_code
                agency_code = new_code
            req_body = {}
            # uac_number = str(row["UAC"]).strip().split(":")[0]
            # uac_name = str(row["UAC"]).strip().split(":")[1]
            uac_number = str(row["UAC"]).strip()
            uac_name = str(row["UAC Name"]).strip()
            if(len(uac_number) == 5):
                new_uac = "000" + uac_number
                uac_number = new_uac
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            member_consolidated_id = ''

            name_string = row["Agency Name"] + " - Nationwide - " + word_date
            agency_name = str(row['Agency Name']).strip(
            ).lower().replace(',', "").replace('.', "")
            Prior_YTD_DWP = int(row["Prior YTD DWP"])
            YTD_DWP = int(row["Curr. YTD DWP"])
            YTD_DWP_Growth = format_percentage(row["YTD DWP Growth"])
            DWP_12MM = int(row["12MM DWP"])
            Prior_YTD_NB_DWP = int(row["Prior YTD DWP"])
            YTD_NB_DWP = int(row["Curr. YTD NB DWP"])
            NB_DWP_12MM = int(row['New DWP 12MM'])
            PIF_NB_12MM = int(row["New PIF 12MM"])
            YTD_PIF = int(row["PIF YTD"])
            Incurred_Loss_12MM = int(row["Incurred Loss 12MM"])
            Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM'])
            Earned_Premium_12MM = int(row["Earned Premium 12MM"])
            Loss_Ratio_12MM = format_percentage(row["12MM Loss Ratio %"])

            try:
                
                # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
                account_id = agency_code_id_map[agency_code]["acc_id"]
                agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
                contact_id = agency_code_id_map[agency_code]["contact_id"]
                # if we have made it to this part of the code, that means that the agency code on this row was indeed in our system, therefore we do not need to make an agency code record.
                # we will now prepare our request object variable to send to the API. To see what API names correspond to what fields in the module go to go to the settings of the CRM > Developer Space > APIs > API Names
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
                            "Carrier": "Nationwide",
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

            req_body = {
                "data": [
                    {
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        'Carrier':'Nationwide',
                        "Uniform_Agency_Code": uac_number,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        "YTD_DWP": YTD_DWP,
                        "Prior_YTD_DWP": Prior_YTD_DWP,
                        'DWP_12MM': DWP_12MM,
                        "YTD_DWP_Growth": YTD_DWP_Growth,
                        "Earned_Premium_12MM": Earned_Premium_12MM,
                        'YTD_NB_DWP': YTD_NB_DWP,
                        'Prior_YTD_NB_DWP':  Prior_YTD_NB_DWP,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'YTD_PIF': YTD_PIF,
                        "PIF_NB_12MM": PIF_NB_12MM,
                        "Incurred_Loss_12MM": Incurred_Loss_12MM,
                        "Capincurred_Loss_12MM":   Capincurred_Loss_12MM,
                        "Loss_Ratio_12MM": Loss_Ratio_12MM,
                    }
                ]

            }

            consolidated_res = requests.post(
                url=consolidated_url, headers=headers, data=json.dumps(req_body))
            if(consolidated_res.json()['data'][0]['status'] == "error"):
                print(req_body)
                print(consolidated_res.json())
                return
            member_consolidated_id = consolidated_res.json()[
                'data'][0]['details']['id']
            member_consolidated_count += 1
        
            # PL Section
            Prior_YTD_DWP = int(row["Prior YTD DWP (PL)"])
            YTD_DWP = int(row["Curr. YTD DWP (PL)"])
            YTD_DWP_Growth = format_percentage(row["YTD DWP Growth (PL)"])
            DWP_12MM = int(row["12MM DWP (PL)"])
            YTD_NB_PIF = int(row['New PIF YTD (PL)'])
            NB_DWP_12MM = int(row['12MM New DWP (PL)'])
            Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM (PL)'])
            Earned_Premium_12MM = int(row['Earned Premium 12MM (PL)'])
            Loss_Ratio_12MM = format_percentage(row['12MM LR (PL)'])
            Line_of_Business = 'Personal'

            lob_name_string = row["Agency Name"] + " - Nationwide - " + word_date + " - " + Line_of_Business

            req_body = {
                'data': [
                    {
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Consolidated_Record': member_consolidated_id,
                        'AC_Total': personal_id,
                        'Name': lob_name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Uniform_Agency_Code':uac_number,
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }
            lob_response = requests.post(
                url=member_lob_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
            if(lob_response.json()['data'][0]['status'] == "error"):
                print(lob_response.json())
                print(req_body)
                return

            # CL Section
            Line_of_Business = 'Commercial'
            lob_name_string = row["Agency Name"] + " - Nationwide - " + word_date + " - " + Line_of_Business
            Prior_YTD_DWP = int(row["Prior YTD DWP (CL/Farm)"])
            YTD_DWP = int(row["Curr. YTD DWP (CL/Farm)"])
            YTD_DWP_Growth = format_percentage(row["YTD DWP Growth (CL/Farm)"])
            DWP_12MM = int(row["12MM DWP (CL/Farm)"])
            YTD_NB_PIF = int(row['New PIF YTD (CL/Farm)'])
            NB_DWP_12MM = int(row['12MM New DWP (CL/Farm)'])
            Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM (CL/Farm)'])
            Earned_Premium_12MM = int(row['Earned Premium 12MM (CL/Farm)'])
            Loss_Ratio_12MM = format_percentage(row['12MM LR (CL/Farm)'])
            req_body = {
                'data': [
                    {
                        'Account': account_id,
                        'Contact': contact_id,
                        'Agency_Code': agency_code_id,
                        'Consolidated_Record': member_consolidated_id,
                        'AC_Total': commercial_id,
                        'Name': lob_name_string,
                        'Report_Type': ['YTD'],
                        'Report_Date': Report_Date,
                        'Line_of_Business': Line_of_Business,
                        'Carrier': 'Nationwide',
                        'Uniform_Agency_Code':uac_number,
                        'Name': lob_name_string,
                        'Prior_YTD_DWP': Prior_YTD_DWP,
                        'YTD_DWP': YTD_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'DWP_12MM': DWP_12MM,
                        'YTD_NB_PIF': YTD_NB_PIF,
                        'NB_DWP_12MM': NB_DWP_12MM,
                        'Capincurred_Loss_12MM': Capincurred_Loss_12MM,
                        'Earned_Premium_12MM': Earned_Premium_12MM,
                        'Loss_Ratio_12MM': Loss_Ratio_12MM

                    }
                ]
            }

            lob_response = requests.post(
                url=member_lob_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
            if(lob_response.json()['data'][0]['status'] == "error"):
                print(lob_response.json())
                print(req_body)
                return

        # Most Recent Function
        client = boto3.client('lambda',region_name='us-east-1')
        data = ''
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:907387566050:function:most_recent_trigger',
            InvocationType='RequestResponse',
            Payload = json.dumps(data)
        )
    
    except Exception as e:
        print(e)
        # Email Notification Function
        data = {"location": "Nationwide(Excel) Parser Function (EC2)"}
        client = boto3.client('lambda',region_name='us-east-1')
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:907387566050:function:failure_email_notification',
            InvocationType='RequestResponse',
            Payload = json.dumps(data)
        )

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