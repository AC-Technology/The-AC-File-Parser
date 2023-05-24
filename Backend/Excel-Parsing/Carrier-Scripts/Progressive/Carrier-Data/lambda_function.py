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
    
    
    auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
        '&grant_type=refresh_token&client_id=' + \
        client_id+'&client_secret=' + client_secret
    

    
    consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
    agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'
    agency_code_search_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes/search'
    member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'


    auth_response = requests.post(url=auth_url)


    access_token = auth_response.json()['access_token']


    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    # read carrier data in using pandas

    carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl', sheet_name = "By Agent")


    account_id_map = {}
    agency_code_id_map = {}

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

    more_records = True
    agency_code_records =[]
    page = 1
    while more_records:
        search_params = {'criteria': 'Carrier:equals:Progressive',
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
                
    carrier_data['Unnamed: 8'] = carrier_data['Unnamed: 8'].fillna("--")
    carrier_data["Unnamed: 21"] = carrier_data["Unnamed: 21"].fillna("--")
    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0

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

    # Create the Carrier Report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Name = 'Progressive - ' + word_date
    DWP_12MM = is_integer_num(carrier_data['Unnamed: 6'].iloc[-1])
    Earned_Premium_12MM = is_integer_num(carrier_data['Unnamed: 7'].iloc[-1])
    Loss_Ratio_12MM = format_number(carrier_data['Unnamed: 8'].iloc[-1])
    Incurred_Loss_12MM = is_integer_num(carrier_data['Unnamed: 9'].iloc[-1])
    YTD_DWP = is_integer_num(carrier_data["Unnamed: 25"].iloc[-1])
    YTD_NB_DWP = is_integer_num(carrier_data['Unnamed: 26'].iloc[-1])
    Earned_Premium_YTD = is_integer_num(carrier_data['Unnamed: 27'].iloc[-1])
    YTD_Loss_Ratio = format_number(carrier_data['Unnamed: 28'].iloc[-1])
    Incurred_Loss_YTD = is_integer_num(carrier_data['Unnamed: 29'].iloc[-1])
    Prior_YTD_DWP = is_integer_num(carrier_data['Unnamed: 30'].iloc[-1])
    YTD_Quotes = int(carrier_data["Unnamed: 19"].iloc[-1])
    YTD_NB_Hit_Ratio = format_number(carrier_data["Unnamed: 21"].iloc[-1])
    request_body = {
            "data": [{
                "Name": Name,
                "Carrier": "Progressive",
                "Report_Date": Report_Date,
                'Report_Type': ['Monthly','YTD'],
                "YTD_DWP": YTD_DWP,
                "YTD_NB_DWP": YTD_NB_DWP,
                "Prior_YTD_DWP": Prior_YTD_DWP,
                "DWP_12MM": DWP_12MM,
                "Earned_Premium_YTD":Earned_Premium_YTD,
                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                "Incurred_Loss_YTD": Incurred_Loss_YTD,
                "Earned_Premium_12MM": Earned_Premium_12MM,
                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                "Incurred_Loss_12MM": Incurred_Loss_12MM,
                "YTD_Quotes": YTD_Quotes,
                "YTD_NB_Hit_Ratio": YTD_NB_Hit_Ratio,
            }]
        }
    carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = (carrier_report.json())
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

    for i, row in carrier_data[12:-1].iterrows():
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        agency_code = str(row["Agency Collective"]).strip()
        agency_name = str(row['Unnamed: 1']).strip()
        name_string = agency_name + " - Progressive - " + word_date
        DWP_12MM = is_integer_num(row['Unnamed: 6'])
        Earned_Premium_12MM = is_integer_num(row['Unnamed: 7'])
        Loss_Ratio_12MM = format_number(row['Unnamed: 8'])
        Incurred_Loss_12MM = is_integer_num(row['Unnamed: 9'])
        YTD_DWP = is_integer_num(row["Unnamed: 25"])
        YTD_NB_DWP = is_integer_num(row['Unnamed: 26'])
        Earned_Premium_YTD = is_integer_num(row['Unnamed: 27'])
        YTD_Loss_Ratio = format_number(row['Unnamed: 28'])
        Incurred_Loss_YTD = is_integer_num(row['Unnamed: 29'])
        Prior_YTD_DWP = is_integer_num(row['Unnamed: 30'])
        YTD_Quotes = int(row["Unnamed: 19"])
        YTD_NB_Hit_Ratio = format_number(row["Unnamed: 21"])
        

        request_body = {}


        try:
            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

        except KeyError:
            formatted_agency_name = agency_name.strip().lower().replace(',', "").replace('.', "")
            try:
                account_id = account_id_map[formatted_agency_name]['acc_id']
                contact_id = account_id_map[formatted_agency_name]['cont_id']
            except KeyError:
                pass

            
            if (account_id == ''):
                account_id = 5187612000000560802

            request_body = {
                "data": [{
                    "Account": account_id,
                    "Name": agency_code,
                    "UAC": uac_number,
                    "Carrier": "Progressive",
                    "UAC_Name": agency_name,
                    "Contact": contact_id
                    # add additional fields here
                }]
            }


            agency_code_resp = requests.post(
                url=agency_code_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))

            if (agency_code_resp.json()['data'][0]['status'] == "error"):
                print("  ")
                print(request_body)
                print(agency_code_resp.json())
                print("  ")
                return
            agency_code_id = agency_code_resp.json()['data'][0]['id']
                 # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

        request_body = {
            "data": [{
                "Name": name_string,
                "Carrier_Report": carrier_report_id,
                "Account": account_id,
                "Agency_Code": agency_code_id,
                "Contact": contact_id,
                "Carrier": "Progressive",
                "Report_Date": Report_Date,
                'Report_Type': ['Monthly','YTD'],
                "YTD_DWP": YTD_DWP,
                "YTD_NB_DWP": YTD_NB_DWP,
                "Prior_YTD_DWP": Prior_YTD_DWP,
                "DWP_12MM": DWP_12MM,
                "Earned_Premium_YTD":Earned_Premium_YTD,
                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                "Incurred_Loss_YTD": Incurred_Loss_YTD,
                "Earned_Premium_12MM": Earned_Premium_12MM,
                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                "Incurred_Loss_12MM": Incurred_Loss_12MM,
                "YTD_Quotes": YTD_Quotes,
                "YTD_NB_Hit_Ratio": YTD_NB_Hit_Ratio,
            }]
        }


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

       
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))
    return



def is_integer_num(n):
    if type(n)== int:
        return n
    if type(n)== float:
        return round(n, 1)


def format_number(num):

    if(not type(num) == str):
        num = float(num)
        num = num*100

        num_final = round(num, 2)

        if(num_final > 9999 and num_final < 99999):
            num_final = round(num_final, 0)
            num_final = int(num_final)
        if(num_final < -9999):
            num_final = -9999
        if(num_final > 9999):
            num_final = 9999
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
