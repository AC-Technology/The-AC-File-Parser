
from multiprocessing.sharedctypes import Value
import pandas as pd
from difflib import SequenceMatcher
import requests
import json
import datetime

client_id = '1000.WI69YDZMVKOI2OVL22NID700CTMNTD'
client_secret = '9689b45819ebb9b16ffab2d96ae9108ebefee9eae5'
refresh_token = '1000.92c7d1df11e9e5b8d259c7edf73fdb88.e918f39048f8f838075705efcd73c28c'


def main():

    auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
        '&grant_type=refresh_token&client_id=' + \
        client_id+'&client_secret=' + client_secret

    auth_headers = {
        'Authorization': 'Bearer: 1000.ac491aaa2e764ea9fbc7d95488a86e3c.3a573f2a5a130e21b06ba19769238d4e'
    }

    auth_response = requests.post(url=auth_url, headers=auth_headers)
    access_token = auth_response.json()['access_token']
    # print(access_token)
    consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
    agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'
    member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'

    headers = {
        'Authorization': 'Zoho-oauthtoken ' + access_token
    }

    nationwide_data = pd.read_excel('NW - Dec 2021 - Formatted.xlsx')

    zoho_account_data = pd.read_csv('../Zoho-Data/Accounts_001.csv')
    zoho_agency_code_data = pd.read_csv('../Zoho-Data/Agency_Codes_C_001.csv')

    account_id_map = {}
    agency_code_id_map = {}

    for i, row in zoho_agency_code_data.iterrows():

        if(row["Carrier"] == "Nationwide"):
            account_id = str(row["Agency ID"]).split("_")[1]
            agency_code = str(row["Agency Code"]).strip()
            agency_code_id_map[agency_code] = {
                "acc_id": account_id,
                "agency_code_id": str(row["Record Id"]).split("_")[1],
            }
            try:
                agency_code_id_map[agency_code]['contact_id'] = str(
                    row["Agency Contact ID"]).split("_")[1]

            except:
                agency_code_id_map[agency_code]['contact_id'] = ""
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

    carrier_report_id = 5187612000001272013
    member_consolidated_count = 0
    new_agency_code_count = 0
    unassigned_count = 0

    count = 0

    report_date = '2021-12-31'
    nationwide_data = nationwide_data.fillna(0)
    nationwide_data = nationwide_data.replace(' ', 0)

    for i, row in nationwide_data.iterrows():
        print(row['Agency Name'])
        # print(row["Agency Name"])

        # first create member consolidated
        # then add member lob with consolidated id and lob total id
        count += 1
        print(count)

        agency_code = str(row["Agency Code"]).strip()
        if(len(agency_code) == 5):
            new_code = "000" + agency_code
            agency_code = new_code
        req_body = {}
        uac_number = str(row["UAC"]).strip().split(":")[0]
        uac_name = str(row["UAC"]).strip().split(":")[1]
        if(len(uac_number) == 5):
            new_uac = "000" + uac_number
            uac_number = new_uac
        # uac_string = str(row["UAC"]).strip()
        # uac_name = str(row["UAC Name"]).strip()
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        member_consolidated_id = ''

        name_string = row["Agency Name"] + " - Nationwide - December 2021"

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

            account_id = agency_code_id_map[agency_code]["acc_id"]
            agency_code_id = agency_code_id_map[agency_code]["agency_code_id"]
            contact_id = agency_code_id_map[agency_code]["contact_id"]

            req_body = {
                "data": [
                    {
                        "Name": name_string,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "Contact": contact_id,
                        "Uniform_Agency_Code": uac_number,
                        'Report_Type': ['Monthly','Year End'],
                        'Report_Date': report_date,
                        'Carrier': 'Nationwide',

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
                            'Carrier': 'Nationwide',

                            "Uniform_Agency_Code": uac_number,
                            'Report_Type': ['Monthly','Year End'],
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
                                'Report_Type': ['Monthly','Year End'],
                                'Report_Date': report_date,
                                'Carrier': 'Nationwide',

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
                                'Report_Type': ['Monthly','Year End'],
                                'Report_Date': report_date,
                                'Carrier': 'Nationwide',

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
        ac_lob_total_id = 5187612000001092017
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
            " - Nationwide - December 2021 - PL"

        req_body = {
            'data': [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Consolidated_Record': member_consolidated_id,
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','Year End'],
                    'Report_Date': report_date,
                    'Line_of_Business': Line_of_Business,
                    'Carrier': 'Nationwide',
                    'Uniform_Agency_Code': uac_number,

                    'Name': lob_name_string,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'YTD_DWP': YTD_DWP,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
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

        ac_lob_total_id = 5187612000001092022
        Line_of_Business = 'Commercial'

        lob_name_string = row["Agency Name"] + \
            " - Nationwide - December 2021 - CL"
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
                    '275': 'Monthly',
                    'Report_Date': report_date,
                    'Line_of_Business': Line_of_Business,
                    'Carrier': 'Nationwide',
                    'Uniform_Agency_Code': uac_number,

                    'Name': lob_name_string,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'YTD_DWP': YTD_DWP,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
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

        # Farm section

        ac_lob_total_id = 5187612000001092027
        Line_of_Business = 'Farm'
        lob_name_string = row["Agency Name"] + \
            " - Nationwide - December 2021 - Farm"
        Prior_YTD_DWP = int(row["Prior YTD DWP (Farm)"])
        YTD_DWP = int(row["Curr. YTD DWP (Farm)"])
        YTD_DWP_Growth = format_percentage(row["YTD DWP Growth (Farm)"])
        DWP_12MM = int(row["12MM DWP (Farm)"])

        YTD_NB_PIF = int(row['New PIF YTD (Farm)'])

        NB_DWP_12MM = int(row['12MM New DWP (Farm)'])
        Capincurred_Loss_12MM = int(row['CapIncurred Loss 12MM (Farm)'])
        Earned_Premium_12MM = int(row['Earned Premium 12MM (Farm)'])
        Loss_Ratio_12MM = format_percentage(row['12MM LR (Farm)'])
        req_body = {
            'data': [
                {
                    'Account': account_id,
                    'Contact': contact_id,
                    'Agency_Code': agency_code_id,
                    'Consolidated_Record': member_consolidated_id,
                    'AC_Total': ac_lob_total_id,
                    'Name': lob_name_string,
                    'Report_Type': ['Monthly','Year End'],
                    'Report_Date': report_date,
                    'Line_of_Business': Line_of_Business,
                    'Carrier': 'Nationwide',
                    'Uniform_Agency_Code': uac_number,

                    'Name': lob_name_string,
                    'Prior_YTD_DWP': Prior_YTD_DWP,
                    'YTD_DWP': YTD_DWP,
                    'YTD_DWP_Growth': YTD_DWP_Growth,
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


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


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
