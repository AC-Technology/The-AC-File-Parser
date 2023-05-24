import pandas as pd
import requests
import json
import boto3
import io

# Global Variables
client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'
coql_url = 'https://www.zohoapis.com/crm/v2/coql'

carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'
agency_code_search_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes/search'
agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

auth_response = requests.post(url=auth_url)

access_token = auth_response.json()['access_token']

headers = {
    'Authorization': 'Zoho-oauthtoken ' + access_token
}

def lambda_handler(event,context):
    try:
        print(event)
        key = event['params']['querystring']['key']
        print(key)
        filename = event['params']['querystring']['filename']
        dot = filename.index(".")
        filename = filename[:dot]

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

        more_records = True
        agency_code_records =[]
        page = 1
        while more_records:
            search_params = {'criteria': 'Carrier:equals:Allstate',
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
        

        # Loop through Agency Code Records
        for i in range(len(agency_code_records)):
            current_record = agency_code_records[i]
            agency_code_id_map[current_record['Name']] = {
                'agency_code_id': current_record['id'],
                'acc_id': current_record['Account']['id'],
            }
            try:
                agency_code_id_map[current_record['Name']][
                    'contact_id'] = current_record['Contact']['id']

            except TypeError:
                agency_code_id_map[current_record['Name']]['contact_id'] = ''

        #Report Date
        words= filename.split()
        list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]

        months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
        days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

        word_date = list_words[-1]
        space = list_words[-1].index(" ")
        month = list_words[-1][:3]
        year = list_words[-1][space:]
        day = days[month]
        month = months[month]

        if month < 10:
            Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
        else:
            Report_Date = str(year) + "-" + str(month) + "-" + str(day)

        carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl', sheet_name='AC Allstate Corporate Scorecard')
        carrier_data.columns = carrier_data.iloc[4]
        carrier_data = carrier_data.iloc[4:]
        print(carrier_data)
        
        Name = 'Allstate - ' + word_date
        YTD_NB_DWP = int(carrier_data["YTD" + str(year)].iloc[2])
        Prior_YTD_New_DWP = int(carrier_data["PYTD"].iloc[2])
        YTD_NB_DWP_Growth = format_number(carrier_data["% ∆"].iloc[2])
        YTD_DWP = int(carrier_data["YTD" + str(year)].iloc[3])
        PYTD_DWP = int(carrier_data["PYTD"].iloc[3])
        YTD_DWP_Growth  = format_number(carrier_data["% ∆"].iloc[3])
        YTD_PIF = int(carrier_data["YTD" + str(year)].iloc[5])
        PYTD_PIF = int(carrier_data["PYTD"].iloc[5])
        YTD_PIF_Growth = format_number(carrier_data["% ∆"].iloc[5])
        YTD_Submits = int(carrier_data["YTD" + str(year)].iloc[4])
        Prior_YTD_Submits = int(carrier_data["PYTD"].iloc[4])
        request_body = {
                        "data": [{
                                "Name": Name,
                                "Carrier": "Allstate",
                                "Report_Type": ['YTD'],
                                "Report_Date": Report_Date,
                                "YTD_NB_DWP": YTD_NB_DWP,
                                "Prior_YTD_New_DWP": Prior_YTD_New_DWP,
                                "YTD_NB_DWP_Growth": YTD_NB_DWP_Growth,
                                "YTD_DWP": YTD_DWP,
                                "YTD_DWP_Growth": YTD_DWP_Growth,
                                "Prior_YTD_DWP": PYTD_DWP,
                                "YTD_PIF": YTD_PIF,
                                "Prior_YTD_PIF": PYTD_PIF,
                                "YTD_PIF_Growth": YTD_PIF_Growth,
                                "YTD_Submits": YTD_Submits,
                                "Prior_YTD_Submits": Prior_YTD_Submits
                        }]
                    }
        print(request_body)
        carrier_report = requests.post(
                        url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = (carrier_report.json())
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

        count = 0
        new_agency_codes_created = 0
        
        carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl', sheet_name='Allstate Producing Agent Detail')
        for i, row in carrier_data[0:].iterrows():
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            uac_number = '' 

            agency_name = row["Agency Name"]
            agency_code = row["Agent #"]

            name_string = agency_name + ' - ' + Name
            NB_PIF_YTD = int(row["YTD NB"])
            Prior_YTD_NB_PIF = int(row["PYTD NB"])
            YTD_Quotes = int(row["YTD Quotes"])
            PYTD_Quotes = int(row["PYTD Quotes"])
            YTD_NB_DWP = int(row["YTD New WP"])
            YTD_DWP = int(row["YTD Total WP"])
            PYTD_DWP = int(row["PYTD Total WP"])
            YTD_PIF = int(row["YTD PIF"])
            Earned_Premium_12MM = int(row["Earned Premium 12MM"])

            try:
                # the first thing we try is to use the agency code on this row of carrier data to find the associated account, contact and agency id. If this agency code is not in the zoho agency code data, there will be an KeyError, handled by the exception part of this statement. If there is an agency code already in the system, the information is stored into the corresponding variables
                account_id = agency_code_id_map[agency_code]["acc_id"]
                contact_id = agency_code_id_map[agency_code]["contact_id"]
                agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]

            except KeyError:
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
                    account_id = 5187612000000754742
                    # we found an account id match so now we just need to create our agency code record.

                request_body = {
                    "data": [{
                        "Account": account_id,
                        "Name": agency_code,
                        "UAC": uac_number,
                        "Carrier": "Allstate",
                        "UAC_Name": agency_name,
                        "Contact": contact_id
                        # add additional fields here
                    }]
                }
                # This is our API call to the CRM API to post a record in the Agency Code Module.
                agency_code_response = requests.post(
                    url=agency_code_url,
                    headers=headers,
                    data=json.dumps(request_body).encode('utf-8'))

                # Note that we assign the agency code id to a new variable, this is to assign the member consolidated record to this agency code correctly.
                
                if (agency_code_response.json()['data'][0]['status'] == "error"):
                    print("  ")
                    print(request_body)
                    print(agency_code_response.json())
                    print("  ")
                    return

                agency_code_id = agency_code_response.json()['data'][0]['details']['id']

                new_agency_codes_created += 1
                # If it returns a successful response, then the created agency code record id is stored into the corresponding variable

            request_body = {
                        "data": [{
                                "Name": name_string,
                                "Carrier": "Allstate",
                                "Report_Type": ['YTD'],
                                "Report_Date": Report_Date,
                                "Carrier_Report": carrier_report_id,
                                "Account": account_id,
                                "Agency_Code": agency_code_id,
                                "Contact": contact_id,
                                "NB_PIF_YTD": NB_PIF_YTD,
                                "Prior_YTD_NB_PIF": Prior_YTD_NB_PIF,
                                "YTD_Quotes": YTD_Quotes,
                                "Prior_YTD_Quotes": PYTD_Quotes,
                                "YTD_NB_DWP": YTD_NB_DWP,
                                "YTD_DWP": YTD_DWP,
                                "Prior_YTD_DWP": PYTD_DWP,
                                "YTD_PIF": YTD_PIF,
                                "Earned_Premium_12MM": Earned_Premium_12MM
                        }]
                    }

            # Now we make our member consolidated record.
            consolidated_response = requests.post(
                url=consolidated_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))
            
            count += 1
            print(count)

            if (consolidated_response.json()['data'][0]['status'] == "error"):
                print("  ")
                print(request_body)
                print(consolidated_response.json())
                print("  ")
                return
                
        print('Consolidated records created: ' + str(count))
        print('Agency Codes created: ' + str(new_agency_codes_created))

        client = boto3.client('lambda')
        data = ''
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:907387566050:function:most_recent_trigger',
            InvocationType='RequestResponse',
            Payload = json.dumps(data)
        )
        return

    except Exception as e:
        print(e)
        # Email Notification Function
        data = {"location": "Allstate Parser Function"}
        client = boto3.client('lambda')
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:907387566050:function:failure_email_notification',
            InvocationType='RequestResponse',
            Payload = json.dumps(data)
        )


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