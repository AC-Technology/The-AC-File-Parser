import pandas as pd
import requests
import json
import io


def main():
    count = 0

    carrier_report_id = 5187612000001333851
    
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

    carrier_data = pd.read_excel('C:\\Users\\rtran\\Documents\\GitHub\\Legacy-Data-Parser\\Nationwide\\Nationwide - The Agency Cluster - January 2022 Results (2)-1.xlsx', sheet_name = "The Agency Cluster")


    account_id_map = {}
    agency_code_id_map = {}
  

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
    unassigned_count = 0

    count = 0

    report_date = '2022-01-31'
    carrier_data = carrier_data.fillna(0)
    carrier_data = carrier_data.replace(' ', 0)
    carrier_data = carrier_data.replace('-', 0)

    carrier_data.columns = carrier_data.iloc[1]
    for i, row in carrier_data.iloc[2:3].iterrows():
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

        name_string = row["Agency Name"] + " - Nationwide - January 2022"

        # print(type(loss_ratio))
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
            agency_id_record = []
            # This is the search function to find all existing account ids and adds them to a account id map
            coql_url = 'https://www.zohoapis.com/crm/v2/coql'
            coql_response = requests.post(url=coql_url,
                                            headers=headers,
                                            data=json.dumps({
                                                'select_query': "select Account, Contact from Agency_Codes where Name = " + str(agency_code)

                                            }))
            agency_id_record.append(coql_response.json()['data'])
            print(agency_id_record[0][0])
            account_id = agency_id_record[0][0]['Account']['id']
            agency_code_id = agency_id_record[0][0]['id']
            contact_id = agency_id_record[0][0]['Contact']['id']
            req_body = {
                "data": [
                    {
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        "Uniform_Agency_Code": uac_number,
                        'Report_Type': ['Monthly'],
                        'Report_Date': report_date,
                        'Carrier':'Nationwide',
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
                        "Capincurred_Loss_12MM": Capincurred_Loss_12MM,
                        "Loss_Ratio_12MM": Loss_Ratio_12MM,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Quotes_12MM": Quotes_12MM
                    }
                ]
            }

            consolidated_res = requests.post(
                url=consolidated_url, headers=headers, data=json.dumps(req_body, default=str).encode('utf-8'))
            member_consolidated_count += 1
            if(consolidated_res.json()['data'][0]['status'] == "error"):
                print(req_body)
                print(consolidated_res.json())
                return
            member_consolidated_count += 1

            member_consolidated_id = consolidated_res.json()[
                'data'][0]['details']['id']

        except KeyError:
            try:

                account_id = agency_code_id_map[uac_number]["acc_id"]
                contact_id = agency_code_id_map[uac_number]["contact_id"]
                req_body = {
                    "data": [
                        {
                            "Account": account_id,
                            "Name": agency_code,
                            "UAC": uac_number,
                            "Carrier": "Nationwide",
                            "UAC_Name": uac_name,
                            "Contact": contact_id
                        }
                    ]
                }

                agency_res = requests.post(
                    url=agency_code_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
                if(agency_res.json()['data'][0]['status'] == "error"):
                    print(req_body)
                    print(agency_res.json())
                    return
                new_agency_code_count += 1
                agency_code_id = agency_res.json()[
                    'data'][0]['details']['id']

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
                            'Report_Type': ['Monthly'],
                            'Report_Date': report_date,
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
                            "YTD_Quotes": row["YTD Quotes"],
                            "Prior_YTD_Quotes": row["Prior YTD Quotes"],
                            "Quotes_12MM":row["12MM Quotes"]
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

            except KeyError:
                try:
                    account_id = account_id_map[agency_name]['acc_id']
                    contact_id = account_id_map[agency_name]['cont_id']
                    req_body = {
                        "data": [
                            {
                                "Account": account_id,
                                "Name": agency_code,
                                "UAC": uac_number,
                                "Carrier": "Nationwide",
                                "UAC_Name": uac_name,
                                "Contact": contact_id


                            }
                        ]
                    }
                    agency_res = requests.post(
                    url=agency_code_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
                    if(agency_res.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(agency_res.json())
                        return
                    new_agency_code_count += 1
                    agency_code_id = agency_res.json()[
                        'data'][0]['details']['id']
                    req_body = {
                        "data": [
                            {
                                "Name": name_string,
                                "Carrier_Report": carrier_report_id,
                                "Account": account_id,
                                "Agency_Code": agency_code_id,
                                "Contact": contact_id,
                                'Report_Type': ['Monthly'],
                                'Report_Date': report_date,
                                'Carrier':'Nationwide',

                                "Uniform_Agency_Code": uac_number,
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
                                "Capincurred_Loss_12MM": Capincurred_Loss_12MM,
                                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                                "YTD_Quotes": row["YTD Quotes"],
                                "Prior_YTD_Quotes": row["Prior YTD Quotes"],
                                "Quotes_12MM":row["12MM Quotes"]
                            }
                        ]
                    }
                    consolidated_res = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(req_body))

                    if(consolidated_res.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(consolidated_res.json())
                        return
                    member_consolidated_count += 1
                    member_consolidated_id = consolidated_res.json()[
                        'data'][0]['details']['id']

                except KeyError:

                    for key in account_id_map:
                        if(agency_name in key):
                            account_id = account_id_map[key]['acc_id']
                            contact_id = account_id_map[key]['cont_id']
                        if(key in agency_name):
                            account_id = account_id_map[key]['acc_id']
                            contact_id = account_id_map[key]['cont_id']
                    if(account_id == ''):
                        account_id = 5187612000000754742

                    req_body = {
                        "data": [
                            {
                                "Account": account_id,
                                "Name": agency_code,
                                "UAC": uac_number,
                                "Carrier": "Nationwide",
                                "UAC_Name": uac_name,
                            "Contact": contact_id


                            }
                        ]
                    }
                    agency_res = requests.post(
                        url=agency_code_url, headers=headers, data=json.dumps(req_body).encode('utf-8'))
                    if(agency_res.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(agency_res.json())
                        return
                    agency_code_id = agency_res.json()[
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
                                'Report_Type': ['Monthly'],
                                'Report_Date': report_date,
                                'Carrier':'Nationwide',

                                "Uniform_Agency_Code": uac_number,
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
                                "Capincurred_Loss_12MM": Capincurred_Loss_12MM,
                                "Loss_Ratio_12MM": Loss_Ratio_12MM,
                                "YTD_Quotes": row["YTD Quotes"],
                                "Prior_YTD_Quotes": row["Prior YTD Quotes"],
                                "Quotes_12MM":row["12MM Quotes"]
                            }
                        ]
                    }
                    consolidated_res = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(req_body))
                    member_consolidated_count += 1
                    if(consolidated_res.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(consolidated_res.json())
                        return
                    member_consolidated_count += 1
                    member_consolidated_id = consolidated_res.json()[
                        'data'][0]['details']['id']

        # PL Section
        ac_lob_total_id = 5187612000001148190
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
            " - Nationwide - January 2022 - PL"

        req_body = {
            'data': [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Consolidated_Record': member_consolidated_id,
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly'],
                    'Report_Date': report_date,
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

        ac_lob_total_id = 5187612000001148195
        Line_of_Business = 'Commercial'

        lob_name_string = row["Agency Name"] + \
            " - Nationwide - January 2022 - CL"
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
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly'],
                    'Report_Date': report_date,
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

      

    print('Agency Codes Created: ' + str(new_agency_code_count))

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