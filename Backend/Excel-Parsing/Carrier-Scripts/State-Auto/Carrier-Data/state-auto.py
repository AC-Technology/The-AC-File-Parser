import pandas as pd
from difflib import SequenceMatcher
import requests
import json


state_auto_data = pd.read_excel('State Auto - Jan 2022 - Formatted.xlsx')
zoho_account_data = pd.read_csv('Accounts_001.csv')
zoho_agency_code_data = pd.read_csv('Agency_Codes_C_001.csv')
client_id = '1000.WI69YDZMVKOI2OVL22NID700CTMNTD'
client_secret = '9689b45819ebb9b16ffab2d96ae9108ebefee9eae5'
refresh_token = '1000.92c7d1df11e9e5b8d259c7edf73fdb88.e918f39048f8f838075705efcd73c28c'
carrier_report_id = 5187612000001184845

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

auth_headers = {
    'Authorization': 'Bearer: 1000.ac491aaa2e764ea9fbc7d95488a86e3c.3a573f2a5a130e21b06ba19769238d4e'
}

auth_response = requests.post(url=auth_url, headers=auth_headers)
access_token = auth_response.json()['access_token']
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
agency_code_url = 'https://www.zohoapis.com/crm/v2/Agency_Codes'

headers = {
    'Authorization': 'Zoho-oauthtoken ' + access_token
}


def main():

    agency_code_id_map = {}
    acc_id_map = {}
    for i, r in zoho_account_data.iterrows():
        account_name = str(r['Account Name']).strip(
        ).lower().replace(',', "").replace('.', "")

        acc_id_map[account_name] = {
            'acc_id': str(r['Record Id']).split('_')[1],
        }
        try:
            acc_id_map[account_name]['cont_id'] = str(
                r['Primary Contact ID']).split('_')[1]
        except IndexError:
            acc_id_map[account_name]['cont_id'] = ''

    for i, r in zoho_agency_code_data.iterrows():
        if(r["Carrier"] == "State Auto"):
            account_id = str(r["Agency ID"]).split("_")[1]
            agency_code = str(r["Agency Code"]).strip()
            agency_code_id_map[agency_code] = {
                "acc_id": account_id,
                "agency_code_id": str(r["Record Id"]).split("_")[1],
            }
            try:
                agency_code_id_map[agency_code]['cont_id'] = str(
                    r["Agency Contact ID"]).split("_")[1]

            except IndexError:
                agency_code_id_map[agency_code]['contact_id'] = ""

    missing_count = 0
    count = 0
    for i, r in state_auto_data.iterrows():

        count += 1
        print(count)
        account_id = ''
        contact_id = ''
        agency_code_id = ''

        state_auto_name = str(
            r['SECONDARY AGENT CODE AND NAME']).split(" ", 1)[1]
        agency_code = str(
            r['SECONDARY AGENT CODE AND NAME']).split(" ", 1)[0]

        name_string = state_auto_name + ' - State Auto - January 2022'
        YTD_DWP = round( r['CYTD WP'],2)
        YTD_DWP_Growth = round(r[11],2)
        YTD_NB_DWP_Growth = round(r[17],2)
        YTD_PIF_Growth = round(r['YTD PIF % Chg '],2)
        YTD_Loss_Ratio = round(r['CYTD Loss Ratio'],2)
        Prior_YTD_DWP = round(r['PYTD WP'],2)
        DWP_12MM = round(r['CYTD R12 WP'], 2)
        Prior_12MM_DWP = round(r['PYE R12 WP'], 2)
        YTD_PIF = round(r['YTD PIF '],2)
        YTD_NB_DWP =  round(r['CYTD NBWP'],2)
        Prior_YTD_NB_DWP = round(r['PYTD NBWP'])


        try:
            # try to match with zoho agency code data
            account_id = agency_code_id_map[agency_code]['acc_id']
            agency_code_id = agency_code_id_map[agency_code]['agency_code_id']
            contact_id = agency_code_id_map[agency_code]['cont_id']

            # agency_code_id = response.json()[
            #     'data'][0]['details']['id']

            req_body = {
                'data': [
                    {
                        'Name': name_string,
                        'Account': account_id,
                        'Report Date': '01/01/2022',
                        'Carrier': 'State Auto',
                        'Most_Recent': True,
                        'Agency_Code': agency_code_id,
                        'Carrier_Report': carrier_report_id,
                        'Primary_Contact': contact_id,

                        'YTD_DWP': YTD_DWP,
                        'Prior_YTD_DWP':Prior_YTD_DWP,
                        'DWP_12MM':DWP_12MM,
                        'Prior_12MM_DWP':Prior_12MM_DWP,
                        'YTD_DWP_Growth':YTD_DWP_Growth,
                        'YTD_NB_DWP:': YTD_NB_DWP,
                        'Prior_YTD_NB_DWP':Prior_YTD_NB_DWP,
                        'YTD_NB_DWP_Growth':YTD_NB_DWP_Growth,
                        'YTD_PIF':YTD_PIF,
                        'YTD_PIF_Growth':YTD_PIF_Growth,
                        'YTD_Loss_Ratio':YTD_Loss_Ratio
                        # 'Loss_Ratio_12MM':loss_final
                    }
                ]

            }
            response = requests.post(
                url=consolidated_url, headers=headers, data=json.dumps(req_body))
            if(response.json()['data'][0]['status'] == "error"):
                print(req_body)
                print(response.json())
            #     print('loss ratio:' + str(r['2022 CY Loss Ratio']))

        except KeyError:

            try:
                # try and match with zoho account data
                search_name = str(r['SECONDARY AGENT CODE AND NAME']).split(" ", 1)[
                    1].strip().lower().replace(',', "").replace('.', "")
                account_id = acc_id_map[search_name]['acc_id']
                agency_code_id = acc_id_map[search_name]['agency_code_id']
                contact_id = acc_id_map[search_name]['cont_id']

                req_body = {
                    'data': [
                        {
                            'Agency': account_id,
                            'Name': agency_code,
                            'Agency_Contact': contact_id,
                            'Carrier': 'State Auto',
                            'UAC': agency_code,
                            'UAC_Name': state_auto_name

                        }
                    ]
                }
                response = requests.post(
                    url=agency_code_url, headers=headers, data=json.dumps(req_body))
                if(response.json()['data'][0]['status'] == "error"):
                    print(req_body)
                    print(response.json())
                agency_code_id = response.json()[
                    'data'][0]['details']['id']
                

                req_body = {
                    'data': [
                        {
                            'Name': name_string,
                            'Account': account_id,
                            'Report Date': '01/01/2022',
                            'Carrier': 'State Auto',
                            'Most_Recent': True,
                            'Agency_Code': agency_code_id,
                            'Carrier_Report': carrier_report_id,
                            'Primary_Contact': contact_id,

                            'YTD_DWP': YTD_DWP,
                            'Prior_YTD_DWP':Prior_YTD_DWP,
                            'DWP_12MM':DWP_12MM,
                            'Prior_12MM_DWP':Prior_12MM_DWP,
                            'YTD_DWP_Growth':YTD_DWP_Growth,
                            'YTD_NB_DWP:': YTD_NB_DWP,
                            'Prior_YTD_NB_DWP':Prior_YTD_NB_DWP,
                            'YTD_NB_DWP_Growth':YTD_NB_DWP_Growth,
                            'YTD_PIF':YTD_PIF,
                            'YTD_PIF_Growth':YTD_PIF_Growth,
                            'YTD_Loss_Ratio':YTD_Loss_Ratio
                            # 'Loss_Ratio_12MM':loss_final
                        }
                    ]

                }
                response = requests.post(
                    url=consolidated_url, headers=headers, data=json.dumps(req_body))
                if(response.json()['data'][0]['status'] == "error"):
                    print(req_body)
                    print(response.json())

            except KeyError:
                for key in acc_id_map:
                    if(state_auto_name in key):
                        account_id = acc_id_map[key]['acc_id']
                        # agency_code_id = acc_id_map[key]['agency_code_id']
                        contact_id = acc_id_map[key]['cont_id']
                    if(key in state_auto_name):
                        account_id = acc_id_map[key]['acc_id']
                        # agency_code_id = acc_id_map[key]['agency_code_id']
                        contact_id = acc_id_map[key]['cont_id']
                if(account_id == ''):
                    missing_count += 1
                    req_body = {
                        'data': [
                            {
                                'Agency': 5187612000000754742,
                                'Name': agency_code,

                                'Carrier': 'State Auto',
                                'UAC': agency_code,
                                'UAC_Name': state_auto_name

                            }
                        ]
                    }
                    response = requests.post(
                        url=agency_code_url, headers=headers, data=json.dumps(req_body))
                    if(response.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(response.json())
                    agency_code_id = response.json()[
                        'data'][0]['details']['id']

                    req_body = {
                        'data': [
                            {
                                'Name': name_string,
                                'Account': 5187612000000754742,
                                'Report Date': '01/01/2022',
                                'Carrier': 'State Auto',
                                'Most_Recent': True,
                                'Agency_Code': agency_code_id,
                                'Carrier_Report': carrier_report_id,


                                'YTD_DWP': YTD_DWP,
                                'Prior_YTD_DWP':Prior_YTD_DWP,
                                'DWP_12MM':DWP_12MM,
                                'Prior_12MM_DWP':Prior_12MM_DWP,
                                'YTD_DWP_Growth':YTD_DWP_Growth,
                                'YTD_NB_DWP:': YTD_NB_DWP,
                                'Prior_YTD_NB_DWP':Prior_YTD_NB_DWP,
                                'YTD_NB_DWP_Growth':YTD_NB_DWP_Growth,
                                'YTD_PIF':YTD_PIF,
                                'YTD_PIF_Growth':YTD_PIF_Growth,
                                'YTD_Loss_Ratio':YTD_Loss_Ratio
                                # 'Loss_Ratio_12MM':loss_final
                            }
                        ]

                    }
                    response = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(req_body))
                    if(response.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(response.json())
                else:
                    req_body = {
                        'data': [
                            {
                                'Agency': account_id,
                                'Name': agency_code,

                                'Carrier': 'State Auto',
                                'UAC': agency_code,
                                'UAC_Name': state_auto_name

                            }
                        ]
                    }
                    response = requests.post(
                        url=agency_code_url, headers=headers, data=json.dumps(req_body))
                    if(response.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(response.json())

                    req_body = {
                        'data': [
                            {
                                'Name': name_string,
                                'Account': account_id,
                                'Report Date': '01/01/2022',
                                'Carrier': 'State Auto',
                                'Most_Recent': True,
                                'Agency_Code': agency_code_id,
                                'Carrier_Report': carrier_report_id,


                                'YTD_DWP': YTD_DWP,
                                'Prior_YTD_DWP':Prior_YTD_DWP,
                                'DWP_12MM':DWP_12MM,
                                'Prior_12MM_DWP':Prior_12MM_DWP,
                                'YTD_DWP_Growth':YTD_DWP_Growth,
                                'YTD_NB_DWP:': YTD_NB_DWP,
                                'Prior_YTD_NB_DWP':Prior_YTD_NB_DWP,
                                'YTD_NB_DWP_Growth':YTD_NB_DWP_Growth,
                                'YTD_PIF':YTD_PIF,
                                'YTD_PIF_Growth':YTD_PIF_Growth,
                                'YTD_Loss_Ratio':YTD_Loss_Ratio
                                # 'Loss_Ratio_12MM':loss_final
                            }
                        ]

                    }
                    response = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(req_body))
                    if(response.json()['data'][0]['status'] == "error"):
                        print(req_body)
                        print(response.json())

    print('missing count: ' + str(missing_count))


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


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
        num_final = 0

    return num_final
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
        num_final = 0
    
    return num_final


main()
