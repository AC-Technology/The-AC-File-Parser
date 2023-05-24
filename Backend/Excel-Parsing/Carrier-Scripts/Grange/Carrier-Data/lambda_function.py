import pandas as pd
import requests
import json
import boto3
import io

def lambda_handler(event, context):
    print(event)
    key = event['params']['querystring']['key']
    print(key)
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


    carrier_data = pd.read_excel('s3://excel-parser-files/' + key, engine='openpyxl',sheet_name="AGENCY COLLECTIVE")


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
        search_params = {'criteria': 'Carrier:equals:Grange', 'page':page
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
                
    carrier_data = carrier_data.fillna(0)
    carrier_data.columns = carrier_data.iloc[2]
    cols=pd.Series(carrier_data.columns)
    for dup in carrier_data.columns[carrier_data.columns.duplicated(keep=False)]: 
            cols[carrier_data.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                            if d_idx != 0 
                                            else dup 
                                            for d_idx in range(carrier_data.columns.get_loc(dup).sum())]
                                            )
    carrier_data.columns=cols

    for i, row in carrier_data.iterrows():
        if i == 5:
            print(row)
    # Copy starting here for carrier report
    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Report_Date = (carrier_data["Agency Number"][1])
    Report_Date = Report_Date.replace(',','')
    name = Report_Date
    space = Report_Date.index(" ")
    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    year = Report_Date[space+1:]
    month = Report_Date[:3]

    day = days[month]
    month = months[month]

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)
        
    Name = "Grange - " + name

    YTD_DWP = int(carrier_data["YTD DWP"][3]) + int(carrier_data["YTD DWP.1"][3]) + int(carrier_data["YTD DWP.2"][3])
    print(YTD_DWP)
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"][3]) + int(carrier_data["Prior YTD DWP.1"][3]) + int(carrier_data["Prior YTD DWP.2"][3])
    DWP_12MM =  int(carrier_data["Rolling 12 DWP"][3]) + int(carrier_data["Rolling 12 DWP.1"][3]) + int(carrier_data["Rolling 12 DWP.2"][3])
    Month_DWP = int(carrier_data["Month DWP"][3]) + int(carrier_data["Month DWP.1"][3]) + int(carrier_data["Month DWP.2"][3])
    YTD_NB_DWP =  int(carrier_data["YTD NB"][3]) + int(carrier_data["YTD NB.1"][3]) + int(carrier_data["YTD NB.2"][3])
    Prior_YTD_NB_DWP = int(carrier_data["Prior YTD NB"][3]) + int(carrier_data["Prior YTD NB.1"][3]) + int(carrier_data["Prior YTD NB.2"][3])
    NB_DWP_12MM = int(carrier_data["Rolling 12 NB"][3]) + int(carrier_data["Rolling 12 NB.1"][3]) + int(carrier_data["Rolling 12 NB.2"][3])
    YTD_PIF = int(carrier_data["YTD PIF"][3]) + int(carrier_data["YTD PIF.1"][3]) + int(carrier_data["YTD PIF.2"][3])
    Prior_YTD_PIF = int(carrier_data["Prior YTD PIF"][3]) + int(carrier_data["Prior YTD PIF.1"][3]) + int(carrier_data["Prior YTD PIF.2"][3])
    YTD_Quotes = int(carrier_data["YTD Quotes"][3]) + int(carrier_data["YTD Quotes.1"][3]) + int(carrier_data["YTD Quotes.2"][3])
    Quotes_12MM = int(carrier_data["Rolling 12 Quotes"][3]) + int(carrier_data["Rolling 12 Quotes.1"][3]) + int(carrier_data["Rolling 12 Quotes.2"][3])
    Prior_YTD_Quotes = int(carrier_data["Quote Count - Prior YTD"][3]) + int(carrier_data["Prior YTD Quotes"][3]) + int(carrier_data["Prior YTD Quotes.1"][3])
    req_body = {
                    "data": [
                        {
                            'Carrier': "Grange",
                            'Name': Name,
                            'Report_Type': ['Monthly','YTD'],
                            'Report_Date': Report_Date,
                            'YTD_DWP': YTD_DWP,
                            'Prior_YTD_DWP': Prior_YTD_DWP,
                            'DWP_12MM': DWP_12MM,
                            'Month_DWP': Month_DWP,
                            'YTD_NB_DWP': YTD_NB_DWP,
                            'Prior_YTD_NB_DWP': Prior_YTD_NB_DWP,
                            'NB_DWP_12MM': NB_DWP_12MM,
                            'YTD_PIF': YTD_PIF,
                            'Prior_YTD_PIF': Prior_YTD_PIF,
                            'YTD_Quotes': YTD_Quotes,
                            'Prior_YTD_Quotes': Prior_YTD_Quotes,
                            'Quotes_12MM': Quotes_12MM
                        }
                    ]
                }
    carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
    carrier_report_id = (carrier_report.json())
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])


    # Copy Starting Here for Agency LOB Total Reports
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Personal'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP"][3])
    Month_DWP = int(carrier_data["Month DWP"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB'][3])
    YTD_PIF = (carrier_data['YTD PIF'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
                    'Name': lob_name_string,
                    'Carrier_'
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
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    pl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])

    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Commercial'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP.1"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP.1"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP.1"][3])
    Month_DWP = int(carrier_data["Month DWP.1"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB.1'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB.1'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB.1'][3])
    YTD_PIF = (carrier_data['YTD PIF.1'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF.1'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR.1'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR.1'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes.1'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes.1'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
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
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    cl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])

    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = ' - Specialty'
    lob_name_string =  Name + Line_of_Business
    YTD_DWP = int(carrier_data["YTD DWP.2"][3])
    Prior_YTD_DWP = int(carrier_data["Prior YTD DWP.2"][3])
    DWP_12MM = int(carrier_data["Rolling 12 DWP.2"][3])
    Month_DWP = int(carrier_data["Month DWP.2"][3])
    YTD_NB_DWP = int(carrier_data['YTD NB.2'][3])
    Prior_YTD_NB_DWP = int(carrier_data['Prior YTD NB.2'][3])
    NB_DWP_12MM = int(carrier_data['Rolling 12 NB.2'][3])
    YTD_PIF = (carrier_data['YTD PIF.2'][3])
    Prior_YTD_PIF = int(carrier_data['Prior YTD PIF.2'][3])
    Loss_Ratio_12MM = format_percentage(carrier_data['Rolling 12 LR.2'][3])
    YTD_Loss_Ratio = format_percentage(carrier_data['YTD LR.2'][3])
    YTD_Quotes = int(carrier_data['YTD Quotes.2'][3])
    Prior_YTD_Quotes = (carrier_data['Prior YTD Quotes.1'][3])
    Quotes_12MM = int(carrier_data['Rolling 12 Quotes.2'][3])
    request_body = {
            "data": [
                {
                    'Carrier': "Grange",
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
    ac_lob = requests.post(
            url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    sl_lob_total_id = (ac_lob_total_id["data"][0]["details"]['id'])
    
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

        name_string = agency_name + ' - ' + Name

        

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
        # these are the variables that store the value of the corresponding cell.
        Line_of_Business = 'Personal'
        lob_name_string = name_string + " - " + Line_of_Business
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
                    'AC_Total': pl_lob_total_id,
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
        # these are the variables that store the value of the corresponding cell.
        Line_of_Business = 'Commercial'
        lob_name_string = name_string + " - " + Line_of_Business
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
                    'AC_Total': cl_lob_total_id,
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
        # these are the variables that store the value of the corresponding cell.
        Line_of_Business = 'Specialty'
        lob_name_string = name_string + " - " + Line_of_Business
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
                    'AC_Total': sl_lob_total_id,
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