import requests
import json

# Global Variables
client_id = '1000.HRS1Y0I5HCSCXSIBFD4K4FREGF8F3M'
client_secret = '4b042561428a1e5414d38fa72dafdc53da0dc43a0a'
refresh_token = '1000.22b7923753b12e462d784fe8621ca95a.c4226fa8405cd5f1c51ba25701facbdb'
coql_url = 'https://www.zohoapis.com/crm/v2/coql'

carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
consolidated_url = 'https://www.zohoapis.com/crm/v2/Member_Data_Consolidated'
lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
member_lob_url = 'https://www.zohoapis.com/crm/v2/Member_LOB_Data'

auth_url = 'https://accounts.zoho.com/oauth/v2/token?refresh_token=' + refresh_token + \
    '&grant_type=refresh_token&client_id=' + \
    client_id+'&client_secret=' + client_secret

auth_response = requests.post(url=auth_url)

access_token = auth_response.json()['access_token']

headers = {
    'Authorization': 'Zoho-oauthtoken ' + access_token
}


def main():
    delete_report_id = "5187612000014984223"

    # Delete agency reports linked to deleted report id
    agency_reports(delete_report_id)
    print("")

    # Delete lob total reports linked to deleted report id and store the lob total ids
    LOB_Total_IDs = lob_total_reports(delete_report_id)
    print("")

    # Delete member lob reports linked to lob total ids 
    member_lob_reports(LOB_Total_IDs)
    print("")

    # # Delete carrier reports linked to deleted report id
    # carrier_report(delete_report_id)
    # print("")

    return

# delete agency reports function
def agency_reports(delete_report_id):
    count = 0
    more_records = True
    query_offset = 0
    reports = []
    list_ids = []
    has_Report = False


    # Grab all the agency reports with the delete report id linked to it
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from Member_Data_Consolidated where Carrier_Report = " + str(delete_report_id) + " limit " + str(query_offset) + ", 100"
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

    print(reports)

    # loop through the reports and grab the ids and store them in list_ids. if the list gets to 100 records then use the api call to delete the records
    if has_Report == True:
        for report in reports:
            for i in report:
                list_ids.append(i['id'])
                count += 1
                # if the length reaches 100 ids then delete the records 
                if len(list_ids) == 100:
                    delete_params = {
                        'ids': list_ids
                    }
                    response = requests.delete(
                        url=consolidated_url, headers=headers, params=delete_params)
                    # print(response.json())
                    # reset list ids
                    list_ids = []
    
    # delete the remaining records
    if has_Report == True:
        try:
            delete_params = {
                'ids': list_ids
            }
            response = requests.delete(
                url=consolidated_url, headers=headers, params=delete_params)
            # print(response.json())
        except:
            pass

    print("Number of Agency Reports deleted:",count)
    return

# delete lob total reports function
def lob_total_reports(delete_report_id):
    more_records = True
    query_offset = 0
    reports = []
    list_ids = []
    LOB_Total_IDs = {}
    has_Report = False
    count = 0

    # Grab all the agency reports with the delete report id linked to it
    while more_records:
        coql_response = requests.post(url=coql_url,
                                        headers=headers,
                                        data=json.dumps({
                                            'select_query': "select Carrier_Report from AC_LOB_Data_Carrier where Carrier_Report = " + str(delete_report_id) + " limit " + str(query_offset) + ", 100"
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

    # loop through the reports and grab the ids and store them in list_ids. if the list gets to 100 records then use the api call to delete the records
    if has_Report == True:
        for report in reports:
            for i in report:
                # Add lob total ids to the hash map to use later to search for member lob reports
                LOB_Total_IDs[i['id']] = i['id']
                list_ids.append(i['id'])
                count += 1
                # if the length reaches 100 ids then delete the records 
                if len(list_ids) == 100:
                    delete_params = {
                        'ids': list_ids
                    }
                    response = requests.delete(
                        url=lob_total_url, headers=headers, params=delete_params)
                    # print(response.json())
                    # reset list ids
                    list_ids = []
                    
    # delete the remaining records
    if has_Report == True:
        try:
            delete_params = {
                                'ids': list_ids
                            }
            response = requests.delete(
                url=lob_total_url, headers=headers, params=delete_params)
            # print(response.json())
        except:
            pass

    print("Number of LOB Total Reports deleted:",count)
    return LOB_Total_IDs

# delete member lob reports function
def member_lob_reports(LOB_Total_IDs):
    has_Report = False
    count = 0

    # Grab all the agency reports with the LOB Total IDs linked to it
    # Loop through each LOB Total ID stored in LOB_Total_IDs
    for i in LOB_Total_IDs:
        more_records = True
        query_offset = 0
        reports = []
        list_ids = []
        while more_records:
            coql_response = requests.post(url=coql_url,
                                            headers=headers,
                                            data=json.dumps({
                                                'select_query': "select AC_Total from Member_LOB_Data where AC_Total = " + str(i) + " limit " + str(query_offset) + ", 100"
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
                print("Carrier does not have any more Member LOB Reports or there is an error")
                more_records = False
                    
        # loop through the reports and grab the ids and store them in list_ids. if the list gets to 100 records then use the api call to delete the records
        if has_Report == True:
            for report in reports:
                for i in report:
                    count += 1
                    list_ids.append(i['id'])
                    # if the length reaches 100 ids then delete the records 
                    if len(list_ids) == 100:
                        delete_params = {
                            'ids': list_ids
                        }
                        response = requests.delete(
                            url=member_lob_url, headers=headers, params=delete_params)
                        # print(response.json())
                        # reset list ids
                        list_ids = []
                        
        # delete the remaining records
        if has_Report == True:
            try:
                delete_params = {
                                    'ids': list_ids
                                }
                response = requests.delete(
                    url=member_lob_url, headers=headers, params=delete_params)
                # print(response.json())
            except:
                pass

    print("Number of Member LOB Reports deleted:",count)
    return

# # delete carrier report founction
# def carrier_report(delete_report_id):
#     # delete carrier report
#     delete_params = {
#                     'ids': delete_report_id
#                 }
#     response = requests.delete(
#         url=carrier_url, headers=headers, params=delete_params)
#     # print(response.json())
#     print('Carrier Report has been deleted')
#     return

main()