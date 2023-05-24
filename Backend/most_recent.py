from calendar import c
from itertools import count
import requests
import pandas as pd
import json
# import boto3

client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'

# this is the authorization url. As you can see it is just an endpoint (URL) that uses the above variables to retrieve an access token.
# Fundamentally, what an API call does in any language (python, javascript), is put the info in the correct place in the URL, access it, and return a response
# based on what has been programmed on the other side of the API to respond.

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

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

carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'
coql_url = 'https://www.zohoapis.com/crm/v2/coql'


def main():
    # This is the search function to find all existing carrier reports
    more_records = True
    query_offset = 0
    reports = []
    # s3 = boto3.resource('s3')
    # obj = s3.Object('textractkvtable-us-east-1-907387566050', 'users.json')
    # data = json.load(obj.get()['Body'])
    # carrier = data["carrier"]
    # carrier = "'" + carrier + "'" 
    data = {"carrier":"Nationwide","filename":"Nationwide+-+July+2022.xlsx","key":"9013640.xlsx"}
    carrier = carrier = data["carrier"].replace('+',' ')
    carrier = "'" + carrier + "'"
    report_name = data["filename"].replace('+',' ')
    report_name = report_name[0:-5]
    if carrier == 'Wyandot':
            carrier = 'Wyandot Mutual'
    print(report_name)
    # print(carrier)

    if carrier == "'Nationwide'":
        if "All Codes" in report_name:
            print("Most Recent Function Not Required")
        elif "Elite Contract" in report_name:
            print("Most Recent Function Not Required")
        elif "Standard Contract" in report_name:
            print("Most Recent Function Not Required")
        elif "All In" in report_name:
            report_name = ""
            Nationwide(report_name)
        else:
            Nationwide(report_name)
    elif carrier == "'Safeco'":
        Safeco(report_name)
    elif carrier == "'Liberty Mutual'":
        LibertyMutual(report_name)
    elif carrier == "'State Auto'":
        StateAuto(report_name)
    elif carrier == "'Travelers'":
        Travelers(report_name)
    else:
        newest_id = Carrier_Reports(carrier,report_name)
        print(newest_id)
        if newest_id != " ":
            Agency_Reports(carrier,newest_id)
            LOB_Total_IDs = LOB_Total_Reports(carrier,newest_id)
            Member_LOB_Reports(carrier,LOB_Total_IDs)
    return

# find most recent for carrier reports
def Carrier_Reports(carrier,report_name):
    query_offset = 0
    reports = []
    more_records = True

    # Uncheck the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100

    # enter array of arrays
    report = reports[0]
    # set most recent to the first report date
    newest_date = report[0]["Report_Date"]
    newest_year = int(newest_date[:4])
    newest_month = newest_date[5:7]
    newest_month = int(newest_month.replace("0",""))
    newest_id = int(report[0]["id"])

    # find the most recent report
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # set the current date to the current hashmap iteration
            current_date = i["Report_Date"]
            current_year =int(current_date[:4])
            current_month = current_date[5:7]
            current_month = int(current_month.replace("0",""))
            current_id = int(i["id"])
            if newest_year < current_year:
                newest_id = current_id
                newest_date = current_date
                newest_month = current_month
                newest_year = current_year
            elif newest_year == current_year:
                if newest_month < current_month:
                    newest_id = current_id
                    newest_date = current_date
                    newest_month = current_month
                    newest_year = current_year
    
    # Update the most recent carrier report
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                        


    # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        newest_id = " "
    return newest_id

# find most recent for agency reports
def Agency_Reports(carrier,newest_id):
    query_offset = 0
    unchecked = 0
    more_records = True
    reports = []
    has_Report = False

    # grab all reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member Reports or there is an error")
            more_records = False

    # unchecking most recent
    if has_Report == True:
        for report in reports:
            for i in report:
                # counter variables
                unchecked += 1
                # update the non recent
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                print(unchecked)
    print("Unchecked Agency Reports:",unchecked)

    Recent_Agency_Reports(newest_id)

    return

# Updates agency reports to the most recent
def Recent_Agency_Reports(newest_id):
    # update most recent records
    count = 0
    more_records = True
    query_offset = 0
    reports = []
    has_Report = False

    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Carrier_Report = " + str(newest_id) + " limit " + str(query_offset) + ", 100"
                                        }))
        found_records = coql_response.json()['data']
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member Reports or there is an error")
            more_records = False

    if has_Report == True:
        for report in reports:
            for i in report:
                count +=1
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                print(count)
    print("Total of most recent Agency Reports:",count)
    return

# find most recent for lob total reports
def LOB_Total_Reports(carrier,newest_id):
    more_records = True
    query_offset = 0
    unchecked = 0
    reports = []
    has_Report = False

    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from AC_LOB_Data_Carrier where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have LOB Total Reports or there is an error")
            more_records = False

    # loop through each report
    if has_Report == True:
        for report in reports:
            # loop through each record in a report and update the most recent
            for i in report:
                unchecked +=1
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                ac_lob = requests.put(
                url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                ac_lob_total_id = (ac_lob.json())
                print(unchecked)
    
    print("Unchecked LOB Total Reports:",unchecked)

    # Call function to update for most recent for lob total reports
    LOB_Total_IDs = Recent_LOB_Total_Reports(newest_id)
    return LOB_Total_IDs

# Updates LOB Total reports to the most recent
def Recent_LOB_Total_Reports(newest_id):
    LOB_Total_IDs = {}
    query_offset = 0
    reports = []
    count = 0
    has_Report = False
    more_records = True
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from AC_LOB_Data_Carrier where Carrier_Report = " + str(newest_id) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have LOB Total Reports or there is an error")
            more_records = False
    if has_Report == True:
        for report in reports:
            for i in report:
                count += 1
                id = i['id']
                # Append most recent LOB IDs to the hashmap
                LOB_Total_IDs[id] = id
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                ac_lob = requests.put(
                url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                ac_lob_total_id = (ac_lob.json())
                print(count)

    print("Total of most recent LOB Totals:",count)
    print(LOB_Total_IDs)

    return LOB_Total_IDs

# find most recent for member lob reports
def Member_LOB_Reports(carrier,LOB_Total_IDs):
    # This is the search function to find all recent member LOB reports
    more_records = True
    query_offset = 0
    reports = []
    has_Report = False
    unchecked = 0

    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select AC_Total from Member_LOB_Data where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Menber LOB Reports or there is an error")
            more_records = False

    if has_Report == True:
        for report in reports:
            for i in report:
                if unchecked == 0:
                    print(i)
                unchecked += 1
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                mem_lob = requests.put(
                        url=member_lob_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                print(unchecked)

    print("Unchecked Member LOB Reports:",unchecked)

    # Call function to update for most recent for member LOB reports
    Recent_Member_LOB_Reports(carrier,LOB_Total_IDs)
    return

# Updates member LOB reports to the most recent
def Recent_Member_LOB_Reports(carrier,LOB_Total_IDs):
    more_records = True
    query_offset = 0
    reports = []
    has_Report = False
    count = 0
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select AC_Total from Member_LOB_Data where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member Reports or there is an error")
            more_records = False

    if has_Report == True:
        for report in reports:
            for i in report:
                # if ac total id of Member LOB report is found in LOB_Total_IDs then change the Member LOB report to most recent
                try:
                    test = LOB_Total_IDs[i["AC_Total"]['id']]
                    id = i['id']
                    request_body = {
                            "data": [{
                                    "id": id,
                                    "Most_Recent": True
                            }]
                        }
                    mem_lob = requests.put(
                    url=member_lob_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                    count += 1
                    print(count)
                except:
                    pass
    print("Total of most recent Member LOB Reports:",count)
    return

# Update most recent for Nationwide
def Nationwide(report_name):
    carrier = "Nationwide"
    newest_ids = {}
    more_records = True

    excel_counter = 0
    allin_counter = 0
    query_offset = 0
    reports = []

    # Search the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100

    # Find the most recent carrier reports
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # find most recent nationwide (excel)
            if "Nationwide - " in i["Name"]:
                if excel_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    excel_month = newest_month
                    excel_year = newest_year
                    newest_excel_id = newest_id

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Excel"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    excel_month = current_month
                    excel_year = current_year
                    newest_excel_id = current_id
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Excel"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        excel_month = current_month
                        excel_year = current_year
                        newest_excel_id = current_id
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Excel"] = current_id
                        newest_month = current_month
                        excel_month = current_month
                        excel_year = current_year
                        newest_excel_id = current_id
                excel_counter += 1

            # find most recent nationwide (all in)
            elif "Nationwide (All In)" in i["Name"]:
                if allin_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    allin_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["All In"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    allin_month = current_month
                    allin_year = current_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["All In"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        allin_month = current_month
                        allin_year = current_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["All In"] = current_id
                        newest_month = current_month
                        allin_month = current_month
                        allin_year = current_year
                allin_counter += 1

    # Check what is more recent. Excel report or All In report?
    # test_map will allow us to find if the current id is in the most recent ids map
    test_map = {}
    newest_id = newest_ids["All In"]
    test_map[newest_id] = newest_id
    if excel_year > allin_year:
        newest_id = newest_excel_id
        test_map = {}
        test_map[newest_id] = newest_id
    elif excel_year == allin_year:
        if excel_month > allin_year:
            newest_id = newest_excel_id
            test_map = {}
            test_map[newest_id] = newest_id
                
    # Update the carrier report to the most recent
    # test_map will allow us to find if the current id is in the most recent ids map
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()
    print(report_uploaded_id,'',newest_id)
     # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        print("The newly uploaded report is not new or considered most recent")
        return

    # call function to update the agency reports
    Nationwide_Agency_Reports(excel_month,excel_year,allin_month,allin_year,newest_ids,newest_excel_id)

    # call function to update the LOB total reports
    LOB_Total_IDs = Nationwide_LOB_Total_Reports(test_map,newest_ids)

    # call function to update the member LOB reports
    Nationwide_Member_LOB_Reports(LOB_Total_IDs)

    return

def Nationwide_Agency_Reports(excel_month,excel_year,allin_month,allin_year,newest_ids,newest_excel_id):
    # Call agency reports function to uncheck the current most recent agency reports
    carrier = 'Nationwide'
    query_offset = 0
    unchecked = 0
    more_records = True
    reports = []
    has_Report = False

    # grab all reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member Reports or there is an error")
            more_records = False

    # unchecking most recent
    if has_Report == True:
        for report in reports:
            for i in report:
                # counter variables
                unchecked += 1
                # update the non recent
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                print(unchecked)
    print("Unchecked Agency Reports:",unchecked)

    # call recent reports function for nationwide
    Nationwide_Recent_Agency_Reports(excel_month,excel_year,allin_month,allin_year,newest_ids,newest_excel_id)

    return

def Nationwide_Recent_Agency_Reports(excel_month,excel_year,allin_month,allin_year,newest_ids,newest_excel_id):
    # This is the search function to find all existing agency reports
    carrier = 'Nationwide'
    has_Report = False
    more_records = True
    query_offset = 0
    reports = []
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Agency Report or there is an error")
            more_records = False
        # print(reports)

    # counter variables
    count = 0
    most_recent_total = 0
    non_recent_total = 0

    # Most recent for agency reports
    # loop through each report
    for report in reports:
        # loop through each record in a report and update the most recent
        for i in report:
            count +=1
            print(count)
            # if excel date and all in date match then change the agency reports to link to the allin carrier report
            if excel_month == allin_month and excel_year == allin_year and int(i["Carrier_Report"]['id']) == newest_ids["Excel"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "id": id,
                                "Carrier_Report": newest_ids["All In"],
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                # print(consolidated_response)

            # # if the excel date is more recent and the carrier report ids match make it most recent but keep the current carrier report id
            # elif excel_month > allin_month and excel_year >= allin_year and int(i["Carrier_Report"]['id']) == newest_ids["Excel"]:
            #     id = (i['id'])
            #     request_body = {
            #             "data": [{
            #                     "id": id,
            #                     "Most_Recent": True
            #             }]
            #         }
            #     consolidated_response = requests.put(
            #                 url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
            #     consolidated_response = consolidated_response.json()
            #     most_recent_total += 1
                # print(consolidated_response)

            # # This elif statement should be used to fix the data if it has already been ran and the agency reports are already linked to the All In Carrier Report the reason not to run this while updating new files because it will keep the current reports as a most recent
            # # if the carrier report id is the All In id then make make it most recent
            # elif int(i["Carrier_Report"]['id']) == newest_ids["All In"]:
            #     id = (i['id'])
            #     request_body = {
            #             "data": [{
            #                     "id": id,
            #                     "Most_Recent": True
            #             }]
            #         }
            #     consolidated_response = requests.put(
            #                 url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
            #     consolidated_response = consolidated_response.json()
            #     most_recent_total += 1
            #     # print(consolidated_response)
                
            else:
                # if the carrier report id matches the newest excel id then make it most recent
                if newest_excel_id == int(i["Carrier_Report"]['id']):
                    id = (i['id'])
                    request_body = {
                            "data": [{
                                    "id": id,
                                    "Most_Recent": True
                            }]
                        }
                    consolidated_response = requests.put(
                                url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                    consolidated_response = consolidated_response.json()
                    most_recent_total += 1
                    # print(consolidated_response)
    print("Total of most recent Agency Reports:",most_recent_total)
    return

def Nationwide_LOB_Total_Reports(test_map,newest_ids):
    carrier = 'Nationwide'
    more_records = True
    query_offset = 0
    unchecked = 0
    reports = []
    has_Report = False

    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from AC_LOB_Data_Carrier where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have LOB Total Reports or there is an error")
            more_records = False

    # loop through each report
    if has_Report == True:
        for report in reports:
            # loop through each record in a report and update the most recent
            for i in report:
                unchecked +=1
                id = i['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                ac_lob = requests.put(
                url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                ac_lob_total_id = (ac_lob.json())
                print(unchecked)
    
    print("Unchecked LOB Total Reports:",unchecked)

    # Call function to update for most recent for lob total reports
    LOB_Total_IDs = Nationwide_Recent_LOB_Total_Reports(test_map,newest_ids)
    return LOB_Total_IDs

def Nationwide_Recent_LOB_Total_Reports(test_map,newest_ids):
    # This is the search function to find all existing LOB total reports
    carrier = "Nationwide"
    has_Report = False
    more_records = True
    query_offset = 0
    reports = []
    LOB_Total_IDs = {}
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Name, Carrier_Report from AC_LOB_Data_Carrier where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have LOB Total Report or there is an error")
            more_records = False
    # print(reports)

    # counter variables
    count = 0
    most_recent_total = 0
    non_recent_total = 0

    # loop through each report
    if has_Report == True:
        for report in reports:
            # loop through each record in a report and update the most recent
            for i in report:
                count +=1
                print(count)
                try:
                    test_map[int(i["Carrier_Report"]['id'])]
                    id = (i['id'])
                    # append the LOB Total ID to the list
                    LOB_Total_IDs[id] = id
                    request_body = {
                            "data": [{
                                    "id": id,
                                    "Most_Recent": True
                            }]
                        }
                    ac_lob = requests.put(
                    url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                    ac_lob_total_id = (ac_lob.json())
                    # print(ac_lob_total_id)
                    most_recent_total += 1
                except:
                    # these are non recent
                    pass
                if "Nationwide - " in i['Name']:
                    if i["Carrier_Report"]['id'] == str(newest_ids["Excel"]):
                        # append the LOB Total ID to the list
                        LOB_Total_IDs[id] = id
        print("Total of most recent LOB Total Reports:",most_recent_total)
        print(LOB_Total_IDs)
    return LOB_Total_IDs
        
def Nationwide_Member_LOB_Reports(LOB_Total_IDs):
    # This is the search function to find all recent member LOB reports
    more_records = True
    query_offset = 0
    unchecked = 0
    carrier = 'Nationwide'
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select AC_Total from Member_LOB_Data where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            found_records = coql_response.json()['data']
            # print(found_records)
            for i in range(len(found_records)):
                # counter variables
                unchecked += 1
                print(unchecked)
                # update the non recent
                id = found_records[i]['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                consolidated_response = requests.put(
                            url=member_lob_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
            query_offset += 100
        except:
            print("Carrier does not have Member LOB Reports or there is an error")
            more_records = False
    # Call function to update for most recent for member LOB reports
    Nationwide_Recent_Member_LOB_Reports(LOB_Total_IDs)
    return

def Nationwide_Recent_Member_LOB_Reports(LOB_Total_IDs):
    # This is the search function to find all existing member LOB reports
    carrier = 'Nationwide'
    has_Report = False
    more_records = True
    query_offset = 0
    reports = []
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select AC_Total from Member_LOB_Data where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(json_response)
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have Member LOB Data or there is an error")
            more_records = False
    # print(reports)

    # counter variables
    count = 0
    most_recent_total = 0
    non_recent_total = 0

    # loop through each report
    if has_Report == True:
        for report in reports:
            # loop through each record in a report and update the most recent
            for i in report:
                count +=1
                print(count)
                try:
                    # print(i["AC_Total"]['id'])
                    test = LOB_Total_IDs[i["AC_Total"]['id']]
                    id = i['id']
                    request_body = {
                            "data": [{
                                    "id": id,
                                    "Most_Recent": True
                            }]
                        }
                    lob_response = requests.put(
                    url=member_lob_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))
                    lob_response = (lob_response.json())
                    # print(lob_response)
                    most_recent_total += 1
                except:
                    pass
        print("Total of most recent Member LOB Reports:",most_recent_total)
    return

# Update most recent for Safeco
def Safeco(report_name):
    carrier = "Safeco"
    newest_ids = {}
    reports = []
    more_records = True

    excel_counter = 0
    ye_counter = 0
    query_offset = 0
    ye_year = 0
    ye_month = 0

    # Search the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100
    
    # Find most recent carrier ids
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # find most recent Safeco (excel)
            if "Safeco - " in i["Name"]:
                if excel_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    excel_month = newest_month
                    excel_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Excel"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    excel_month = newest_month
                    excel_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Excel"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        excel_month = newest_month
                        excel_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Excel"] = current_id
                excel_counter += 1

            # find most recent Safeco (YE Results)
            elif "Safeco (YE Results)" in i["Name"]:
                if ye_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    ye_month = newest_month
                    ye_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["YE Results"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    ye_month = newest_month
                    ye_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["YE Results"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        ye_month = newest_month
                        ye_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["YE Results"] = current_id
                ye_counter += 1

    # Check what is more recent. Excel report or YE Results report?
    newest_id = newest_ids["Excel"]
    if ye_year > excel_year:
        newest_id = newest_ids["YE Results"]
    elif ye_year == excel_year:
        if ye_month > excel_month:
            newest_id = newest_ids["YE Results"]
            

    # Update the carrier report to the most recent
    # test_map will allow us to find if the current id is in the most recent ids map
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()

     # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        print("The newly uploaded report is not new or considered most recent")
        return
    
    # set the newest id to excel's newest id
    newest_id = newest_ids["Excel"]

    # # Call for the agency reports function
    Agency_Reports(carrier,newest_id)

    # Call for the LOB total reports function
    LOB_Total_IDs = LOB_Total_Reports(carrier,newest_id)

    # Call for the member lob reports function
    Member_LOB_Reports(carrier,LOB_Total_IDs)

    return

# Update most recent for Liberty Mutual
def LibertyMutual(report_name):
    carrier = "'Liberty Mutual'"
    newest_ids = {}
    reports = []
    more_records = True

    excel_counter = 0
    ye_counter = 0
    ye_year = 0
    ye_month = 0
    query_offset = 0

    # Search the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100
    
    # Find most recent carrier ids
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # find most recent Liberty Mutual (excel)
            if "Liberty Mutual - " in i["Name"]:
                if excel_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    excel_month = newest_month
                    excel_year = newest_year
                    

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Excel"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    excel_month = newest_month
                    excel_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Excel"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        excel_month = newest_month
                        excel_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Excel"] = current_id
                excel_counter += 1

            # find most recent Safeco (YE Results)
            elif "Liberty Mutual (YE Results)" in i["Name"]:
                if ye_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    ye_month = newest_month
                    ye_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["YE Results"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    ye_month = newest_month
                    ye_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["YE Results"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        ye_month = newest_month
                        ye_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["YE Results"] = current_id
                ye_counter += 1
    
    # Check what is more recent. Excel report or YE Results report?
    newest_id = newest_ids["Excel"]
    if ye_year > excel_year:
        newest_id = newest_ids["YE Results"]
    elif ye_year == excel_year:
        if ye_month > excel_month:
            newest_id = newest_ids["YE Results"]

   # Update the carrier report to the most recent
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()

     # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        print("The newly uploaded report is not new or considered most recent")
        return
    
    # set the newest id to excel's newest id
    newest_id = newest_ids["Excel"]

    # Call for the agency reports function
    Agency_Reports(carrier,newest_id)

    # Call for the Liberty Mutual LOB total reports function
    LOB_Total_IDs = LOB_Total_Reports(carrier,newest_id)

    # Call for the Liberty Mutual member lob reports function
    Member_LOB_Reports(carrier,LOB_Total_IDs)

# Update most recent for State Auto
def StateAuto(report_name):
    carrier = "'State Auto'"
    newest_ids = {}
    reports = []
    more_records = True

    query_offset = 0
    excel_counter = 0
    Production_counter = 0
    farm_counter = 0
    personal_counter = 0
    commercial_counter = 0

    # Search the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100
    
    # Find most recent carrier ids
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # find most recent State Auto (excel)
            if "State Auto - " in i["Name"]:
                if excel_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    excel_month = newest_month
                    excel_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Excel"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    excel_month = newest_month
                    excel_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Excel"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        excel_month = newest_month
                        excel_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Excel"] = current_id
                excel_counter += 1

            # find most recent State Auto (Production & Loss Summary)
            elif "State Auto (Production & Loss Summary)" in i["Name"]:
                if Production_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    pl_month = newest_month
                    pl_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Production"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    pl_month = newest_month
                    pl_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Production"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        pl_month = newest_month
                        pl_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Production"] = current_id
                Production_counter += 1

            # # find most recent State Auto (Farm)
            # elif "State Auto (Farm)" in i["Name"]:
            #     if farm_counter == 0:
            #         newest_date = i["Report_Date"]
            #         newest_year = int(newest_date[:4])
            #         newest_month = newest_date[5:7]
            #         newest_month = int(newest_month.replace("0",""))
            #         newest_id = int(i["id"])

            #     # set the current date to the current hashmap iteration
            #     current_date = i["Report_Date"]
            #     current_year =int(current_date[:4])
            #     current_month = current_date[5:7]
            #     current_month = int(current_month.replace("0",""))
            #     current_id = int(i["id"])

            #     if newest_year < current_year:
            #         newest_ids["Farm"] = current_id
            #         newest_date = current_date
            #         newest_month = current_month
            #     elif newest_year == current_year:
            #         if newest_month < current_month:
            #             newest_ids["Farm"] = current_id
            #             newest_date = current_date
            #             newest_month = current_month
            #     if newest_year == current_year:
            #         if newest_month == current_month:
            #             newest_ids["Farm"] = current_id
            #     farm_counter += 1

            # # find most recent State Auto (Personal)
            # elif "State Auto (Personal)" in i["Name"]:
            #     if personal_counter == 0:
            #         newest_date = i["Report_Date"]
            #         newest_year = int(newest_date[:4])
            #         newest_month = newest_date[5:7]
            #         newest_month = int(newest_month.replace("0",""))
            #         newest_id = int(i["id"])

            #     # set the current date to the current hashmap iteration
            #     current_date = i["Report_Date"]
            #     current_year =int(current_date[:4])
            #     current_month = current_date[5:7]
            #     current_month = int(current_month.replace("0",""))
            #     current_id = int(i["id"])

            #     if newest_year < current_year:
            #         newest_ids["Personal"] = current_id
            #         newest_date = current_date
            #         newest_month = current_month
            #     elif newest_year == current_year:
            #         if newest_month < current_month:
            #             newest_ids["Personal"] = current_id
            #             newest_date = current_date
            #             newest_month = current_month
            #     if newest_year == current_year:
            #         if newest_month == current_month:
            #             newest_ids["Personal"] = current_id
            #     personal_counter += 1

            # # find most recent State Auto (Commercial)
            # elif "State Auto (Commercial)" in i["Name"]:
            #     if commercial_counter == 0:
            #         newest_date = i["Report_Date"]
            #         newest_year = int(newest_date[:4])
            #         newest_month = newest_date[5:7]
            #         newest_month = int(newest_month.replace("0",""))
            #         newest_id = int(i["id"])

            #     # set the current date to the current hashmap iteration
            #     current_date = i["Report_Date"]
            #     current_year =int(current_date[:4])
            #     current_month = current_date[5:7]
            #     current_month = int(current_month.replace("0",""))
            #     current_id = int(i["id"])

            #     if newest_year < current_year:
            #         newest_ids["Commercial"] = current_id
            #         newest_date = current_date
            #         newest_month = current_month
            #     elif newest_year == current_year:
            #         if newest_month < current_month:
            #             newest_ids["Commercial"] = current_id
            #             newest_date = current_date
            #             newest_month = current_month
            #     if newest_year == current_year:
            #         if newest_month == current_month:
            #             newest_ids["Commercial"] = current_id
            #     commercial_counter += 1


    
    # Check what is more recent. Excel report or YE Results report?
    newest_id = newest_ids["Excel"]
    if pl_year > excel_year:
        newest_id = newest_ids["PL"]
    elif pl_year == excel_year:
        if pl_month > excel_month:
            newest_id = newest_ids["PL"]

    # Update the carrier report to the most recent
    # test_map will allow us to find if the current id is in the most recent ids map
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()

     # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        print("The newly uploaded report is not new or considered most recent")
        return
    
    # set the newest id to excel's newest id
    newest_id = newest_ids["Excel"]
    
    # Call for the agency reports function
    Agency_Reports(carrier,newest_id)

    # Call for the LOB total reports function
    LOB_Total_IDs = LOB_Total_IDs(carrier,newest_ids)

    # Call for the member lob reports function
    Member_LOB_Reports(carrier,LOB_Total_IDs)

    return

# Update most recent for Travelers
def Travelers(report_name):
    carrier = "'Travelers'"
    summary_counter = 0
    selection_counter = 0
    sub_counter = 0
    query_offset = 0
    newest_ids = {}
    newest_code_ids = {}
    reports = []
    more_records = True

    # Search the current most recent carrier report
    coql_response = requests.post(url=coql_url,
                                    headers=headers,
                                    data=json.dumps({
                                        "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                    }))
    json_response = coql_response.json()
    current_recent_id = json_response['data'][0]['id']

    # Update the current recent carrier report to non recent
    request_body = {
                "data": [{
                        "id": current_recent_id,
                        "Most_Recent": False
                        }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))

    # Grab all the carrier reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            "select_query": "select Report_Date, Name from Carrier_Reports where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        # print(coql_response)
        json_response = coql_response.json()
        # print(json_response)
        if(not coql_response.json()['info']['more_records']):
            more_records = False
        reports.append(coql_response.json()['data'])
        query_offset += 100

    # Find most recent carrier ids
    for report in reports:
        for i in report:
            if i["Name"] == report_name:
                report_uploaded_id = i['id']
            # find most recent Travelers (Agency Summary)
            if "Travelers (Agency Summary) - " in i["Name"]:
                if summary_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    summary_month = newest_month
                    summary_year = current_year


                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_ids["Summary"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    summary_month = current_month
                    summary_year = current_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_ids["Summary"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        summary_month = current_month
                        summary_year = current_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_ids["Summary"] = current_id
                        summary_month = current_month
                        summary_year = current_year
                summary_counter += 1

             # find most recent Travelers (Sub-Code)
            elif "Travelers (Sub-Code) - " in i["Name"]:
                if selection_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    sub_code_month = newest_month
                    sub_code_year = newest_year

                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_code_ids["Sub-Code"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    sub_code_month = current_month
                    sub_code_year = current_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_code_ids["Sub-Code"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        sub_code_month = current_month
                        sub_code_year = current_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_code_ids["Sub-Code"] = current_id
                        sub_code_month = current_month
                        sub_code_year = current_year
                sub_counter += 1

            # find most recent Travelers (Agency Selection)
            elif "Travelers (Agency Selection) - " in i["Name"]:
                if selection_counter == 0:
                    newest_date = i["Report_Date"]
                    newest_year = int(newest_date[:4])
                    newest_month = newest_date[5:7]
                    newest_month = int(newest_month.replace("0",""))
                    newest_id = int(i["id"])
                    selection_month = newest_month
                    selection_year = newest_year
                    
                # set the current date to the current hashmap iteration
                current_date = i["Report_Date"]
                current_year =int(current_date[:4])
                current_month = current_date[5:7]
                current_month = int(current_month.replace("0",""))
                current_id = int(i["id"])

                if newest_year < current_year:
                    newest_code_ids["Selection"] = current_id
                    newest_date = current_date
                    newest_month = current_month
                    selection_month = newest_month
                    selection_year = newest_year
                elif newest_year == current_year:
                    if newest_month < current_month:
                        newest_code_ids["Selection"] = current_id
                        newest_date = current_date
                        newest_month = current_month
                        selection_month = newest_month
                        selection_year = newest_year
                if newest_year == current_year:
                    if newest_month == current_month:
                        newest_code_ids["Selection"] = current_id
                        selection_month = newest_month
                        selection_year = newest_year
                selection_counter += 1 
    
    # Update the carrier report to the most recent
    # test_map will allow us to find if the current id is in the most recent ids map
    newest_id = newest_ids["Summary"]
    # Update the carrier report to the most recent
    # test_map will allow us to find if the current id is in the most recent ids map
    request_body = {
                "data": [{
                        "id": newest_id,
                        "Most_Recent": True
                }]
            }
    carrier_report = requests.put(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()

    # If the current carrier report id is the newest id then it is a new carrier report id
    if int(report_uploaded_id) == newest_id:
        print("Carrier Report uploaded id is the newest carrier report id")
    else:
        print("The newly uploaded report is not new or considered most recent")
        return
    
    # Call Agency Reports function
    Travelers_Agency_Reports(sub_code_month,summary_month,newest_code_ids,newest_ids,selection_month,sub_code_year,summary_year,selection_year)

    # Call LOB Total Reports function    
    LOB_Total_IDs = LOB_Total_Reports(carrier,newest_id)

    # Call member lob reports function
    Member_LOB_Reports(carrier,LOB_Total_IDs)

def Travelers_Agency_Reports(sub_code_month,summary_month,newest_code_ids,newest_ids,selection_month,sub_code_year,summary_year,selection_year):
    carrier = 'Travelers'
    query_offset = 0
    count = 0
    unchecked = 0
    more_records = True
    # First, uncheck the most recent reports
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Most_Recent = 'true' and Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            found_records = coql_response.json()['data']
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            for i in range(len(found_records)):
                # counter variables
                unchecked += 1
                print(unchecked)
                # update the non recent
                id = found_records[i]['id']
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": False
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                # print(consolidated_response)
            query_offset += 100
        except:
            print("Carrier does not have Agency Report or there is an error")
            more_records = False
    print("Unchecked most recent for:",unchecked)
    # Call function to update for most recent for agency reports
    Travelers_Recent_Agency_Reports(sub_code_month,summary_month,newest_code_ids,newest_ids,selection_month,sub_code_year,summary_year,selection_year)

def Travelers_Recent_Agency_Reports(sub_code_month,summary_month,newest_code_ids,newest_ids,selection_month,sub_code_year,summary_year,selection_year):
    # This is the search function to find all existing agency reports
    carrier = 'Travelers'
    has_Report = False
    more_records = True
    query_offset = 0
    reports = []
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Carrier = " + str(carrier) + " limit " + str(query_offset) + ", 100"
                                        }))
        try:
            json_response = coql_response.json()
            # print(json_response)
            if(not coql_response.json()['info']['more_records']):
                more_records = False
            reports.append(coql_response.json()['data'])
            query_offset += 100
            has_Report = True
        except:
            print("Carrier does not have LOB Total Report or there is an error")
            more_records = False

    # counter variables
    count = 0
    most_recent_total = 0
    non_recent_total = 0

    # Most recent for agency reports
    # loop through each report
    for report in reports:
        # loop through each record in a report and update the most recent
        for i in report:
            # if the sub code date and summary date match and the carrier report id matches then link the agency reports to the summary carrier report (main carrier report) and make it most recent
            if sub_code_month == summary_month and int(i["Carrier_Report"]['id']) == newest_code_ids["Sub-Code"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "Carrier_Report": newest_ids["Summary"],
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                print(most_recent_total)

            # if the selection date and summary date match and the carrier report id matches then link the agency reports to the summary carrier report (main carrier report) and make it most recent
            elif selection_month == summary_month and int(i["Carrier_Report"]['id']) == newest_code_ids["Selection"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "Carrier_Report": newest_ids["Summary"],
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                print(most_recent_total)
        
            # # This elif statement should be used to fix the data if it has already been ran and the agency reports are already linked to the Summary Carrier Report the reason not to run this while updating new files because it will keep the current reports as a most recent
            # # if the carrier report id is the summary id then make make it most recent
            # elif int(i["Carrier_Report"]['id']) == newest_ids["Summary"]:
            #     id = (i['id'])
            #     request_body = {
            #             "data": [{
            #                     "Carrier_Report": newest_ids["Summary"],
            #                     "id": id,
            #                     "Most_Recent": True
            #             }]
            #         }
            #     consolidated_response = requests.put(
            #                 url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
            #     consolidated_response = consolidated_response.json()
            #     most_recent_total += 1
            #     print(most_recent_total)

            # if the sub code date is more recent than the summary's and the carrier id matches then make it most recent but keep it linked to the same carrier report
            elif sub_code_month > summary_month and sub_code_year >= summary_year and  int(i["Carrier_Report"]['id']) == newest_code_ids["Sub-Code"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                print(most_recent_total)

            # if the selection date is more recent than the summary's and the carrier id matches then make it most recent but keep it linked to the same carrier report 
            elif selection_month > summary_month and selection_year >= summary_year and int(i["Carrier_Report"]['id']) == newest_code_ids["Selection"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                print(most_recent_total)
            
            # if the summary date is more recent, make the newest summary agency reports most recent (in other words keep the newest agency reports most recent)
            elif summary_month > sub_code_month and summary_year >= sub_code_year and summary_month > selection_month and summary_year >= selection_year and int(i["Carrier_Report"]['id']) == newest_ids["Summary"]:
                id = (i['id'])
                request_body = {
                        "data": [{
                                "id": id,
                                "Most_Recent": True
                        }]
                    }
                consolidated_response = requests.put(
                            url=consolidated_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
                consolidated_response = consolidated_response.json()
                most_recent_total += 1
                print(most_recent_total)
    print("Total of most recent Agency Reports:",most_recent_total)


main()