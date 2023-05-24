import pandas as pd
import requests
import json
import io
import sys
import boto3


s3 = boto3.client('s3')

def main():
    # Receive parameters from command line
    foldername = sys.argv[3]
    filename = sys.argv[1] + " " + sys.argv[2]
    filename = filename.replace("+", " ")
    # filename = 'Travelers -+Sub-Code++-+December+2021.pdf'
    # filename = filename.replace("+", " ")
    # foldername = '7185650.pdf'
    dot = filename.index(".")
    filename = filename[:dot]
    print(filename)
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
        search_params = {'criteria': 'Carrier:equals:Travelers',
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
    print(list_words)


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

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'

    if "Production Data Summary" in filename:
        Name = "Travelers (Production Data Summary) - " + word_date

         # In lambda, modify it to append every table to the df in a loop without manually stating which file
        try:
            df = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table2.csv")
        except:
            df = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table1.csv")
        df.columns = df.iloc[0]

        YTD_PIF = currency_format(df["YTD"].iloc[8])
        YTD_DWP = currency_format(df["YTD"].iloc[4]) + '000'
        YTD_Loss_Ratio = currency_format(df["YTD"].iloc[24])
        YTD_Quotes = currency_format(df["YTD"].iloc[16])
        Prior_YTD_PIF = currency_format(df["PYTD"].iloc[8])
        Prior_YTD_Quotes = currency_format(df["PYTD"].iloc[16])
        Prior_YTD_DWP = currency_format(df["PYTD"].iloc[4]) + '000'
        request_body = {
                "data": [{
                        "Name": Name,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = carrier_report.json()
        # print(carrier_report_id)
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

        # create Auto LOB report
        lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
        Line_of_Business = 'Auto'
        name_string = Name + ' - ' + Line_of_Business
        YTD_PIF = currency_format(df["YTD"].iloc[5])
        YTD_DWP = currency_format(df["YTD"].iloc[1])
        YTD_Loss_Ratio = currency_format(df["YTD"].iloc[21])
        YTD_Quotes = currency_format(df["YTD"].iloc[13])
        Prior_YTD_PIF = currency_format(df["PYTD"].iloc[5])
        Prior_YTD_Quotes = currency_format(df["PYTD"].iloc[13])
        Prior_YTD_DWP = currency_format(df["PYTD"].iloc[1])
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        "Report_Date": Report_Date,
                        'Line_of_Business': Line_of_Business,
                        "YTD_PIF": YTD_PIF,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())
        # print(ac_lob_total_id)

        # create Home LOB report
        Line_of_Business = "Home"
        name_string = Name +  ' - Home'
        YTD_PIF = currency_format(df["YTD"].iloc[6])
        YTD_DWP = currency_format(df["YTD"].iloc[2])
        YTD_Loss_Ratio = currency_format(df["YTD"].iloc[22])
        YTD_Quotes = currency_format(df["YTD"].iloc[14])
        Prior_YTD_PIF = currency_format(df["PYTD"].iloc[6])
        Prior_YTD_Quotes = currency_format(df["PYTD"].iloc[14])
        Prior_YTD_DWP = currency_format(df["PYTD"].iloc[2])
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())

        # create OPL LOB report
        Line_of_Business = "OPL"
        name_string = Name + ' - OPL'
        YTD_PIF = currency_format(df["YTD"].iloc[7])
        YTD_DWP = currency_format(df["YTD"].iloc[3])
        YTD_Loss_Ratio = currency_format(df["YTD"].iloc[23])
        YTD_Quotes = currency_format(df["YTD"].iloc[15])
        Prior_YTD_PIF = currency_format(df["PYTD"].iloc[7])
        Prior_YTD_Quotes = currency_format(df["PYTD"].iloc[15])
        Prior_YTD_DWP = currency_format(df["PYTD"].iloc[3])
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())

    elif "Sub-Code" in filename:
        Name = 'Travelers (Sub-Code) - ' + word_date

        # In lambda, modify it to append every table to the df in a loop without manually stating which file
        table1 = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table1.csv")
        df = table1
        table_count = 1
        while table_count < 50:
            try:
                table_count += 1 
                table = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table"+str(table_count)+".csv")
                df = df.append(table, ignore_index=True)
            except:
                pass

        df.columns = df.iloc[0]
        cols=pd.Series(df.columns)
        
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]

        # rename the columns with the cols list.
        df.columns=cols
        df = df.fillna("")

        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        # print(df.tail(30))


        YTD_DWP = currency_format(str(df['Written Premium'].iloc[-1])) + "00"
        YTD_PIF = currency_format(str(df['Policies in Force'].iloc[-1]))
        YTD_Loss_Ratio = currency_format(str(df['Loss Ratio'].iloc[-1]))
        YTD_Quotes = currency_format(str(df["Quotes"].iloc[-1]))
        Prior_YTD_PIF = currency_format(str(df["Policies in Force.1"].iloc[-1]))
        Prior_YTD_DWP = currency_format(str(df["Written Premium.1"].iloc[-1])) + "00"
        # create a carrrier report
        request_body = {
                "data": [{
                        "Name": Name,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "YTD_DWP": YTD_DWP,
                        "YTD_PIF": YTD_PIF,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = carrier_report.json()
        # print(carrier_report_id)
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])
    
        new_agency_code_count = 0
        member_consolidated_count = 0
        count = 0

        # Create member records
        for i, row in df.iterrows():
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            member_consolidated_id = ''
            name_string = ''
            count += 1
            print(count)
            if row["Agency Location"] != " " and row["Sub-Code"] != "Sub-Code": 
                agency_code = row["Sub-Code"]
                agency_name = row["Agency Name"]
                name_string = agency_name + " - " + Name
                YTD_DWP = currency_format(str(row['Written Premium']))
                YTD_PIF = currency_format(str(row['Policies in Force']))
                YTD_Loss_Ratio = currency_format(str(row['Loss Ratio']))
                YTD_Quotes = currency_format(str(row["Quotes"]))
                Prior_YTD_PIF = currency_format(str(row["Policies in Force.1"]))
                Prior_YTD_DWP = currency_format(str(row["Written Premium.1"]))
                
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
                    if(account_id == ''):
                        # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.

                        account_id = 5187612000000754742

                        # we found an account id match so now we just need to create our agency code record
                        request_body = {
                            "data": [
                                {
                                    "Account": account_id,
                                    "Name": agency_code,
                                    "Carrier": "Travelers",
                                    "UAC_Name": agency_name,
                                    "Contact": contact_id
                                    # add additional fields here
                                }
                            ]
                        }
                        # this is our API call to the CRM API to post a record in the Agency Code Module.
                        agency_code_response = requests.post(
                            url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

                        if(agency_code_response.json()['data'][0]['status'] == "error"):

                            print("  ")
                        
                            print("  ")
                            return
                        
                        # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

                        agency_code_id = agency_code_response.json()[
                            'data'][0]['details']['id']
                        new_agency_code_count += 1
                
                request_body = {
                        "data": [{
                                "Name": name_string,
                                "Carrier": "Travelers",
                                "Report_Type": ['YTD'],
                                "Report_Date": Report_Date,
                                "Account": account_id,
                                "Agency_Code": agency_code_id,
                                "Carrier_Report": carrier_report_id,
                                "YTD_DWP": YTD_DWP,
                                "YTD_PIF": YTD_PIF,
                                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                                "YTD_Quotes": YTD_Quotes,
                                "Prior_YTD_PIF": Prior_YTD_PIF,
                                "Prior_YTD_DWP": Prior_YTD_DWP
                        }]
                    }
                consolidated_response = requests.post(
                url=consolidated_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))
                member_consolidated_count += 1
                if (consolidated_response.json()['data'][0]['status'] == "error"):
                    print("  ")
                    print(request_body)
                    print(consolidated_response.json())
                    print("  ")
                    return

        
        print('Consolidated records created: ' + str(member_consolidated_count))
        print('Agency Codes created: ' + str(new_agency_code_count))

    elif "Agency Summary" in filename or "YE" in filename:
        if "Agency Summary" in filename:
            Name = 'Travelers (Agency Summary) - ' + word_date
        else:
            Name = 'Travelers (YE Results) - ' + word_date

        # In lambda, modify it to append every table to the df in a loop without manually stating which file
        df = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table1.csv")

        YTD_PIF = currency_format(str(df.loc[3][8]))
        YTD_PIF_Growth = currency_format(str(df.loc[7][8]))
        YTD_DWP = currency_format(str(df.loc[29][8]))
        YTD_Loss_Ratio = currency_format(str(df.loc[33][8]))
        YTD_Quotes = currency_format(str(df.loc[20][8]))
        Prior_YTD_PIF = currency_format(str(df.loc[3][9]))
        Prior_YTD_Quotes = currency_format(str(df.loc[20][9]))
        Prior_YTD_DWP = currency_format(str(df.loc[29][9]))
        request_body = {
                "data": [{
                        "Name": Name,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_PIF_Growth": YTD_PIF_Growth,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        carrier_report = requests.post(
                    url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = carrier_report.json()
        # print(carrier_report_id)
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

        # create Auto LOB report
        lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
        Line_of_Business = 'Auto'
        name_string = Name + ' - ' + Line_of_Business
        YTD_PIF = currency_format(str(df.loc[0][8]))
        YTD_PIF_Growth = currency_format(str(df.loc[4][8]))
        YTD_DWP = currency_format(str(df.loc[26][8]))
        YTD_Loss_Ratio = currency_format(str(df.loc[30][8]))
        YTD_Quotes = currency_format(str(df.loc[17][8]))
        Prior_YTD_PIF = currency_format(str(df.loc[0][9]))
        Prior_YTD_Quotes = currency_format(str(df.loc[17][9]))
        Prior_YTD_DWP = currency_format(str(df.loc[26][9]))
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        "Report_Date": Report_Date,
                        'Line_of_Business': Line_of_Business,
                        "YTD_PIF": YTD_PIF,
                        "YTD_PIF_Growth": YTD_PIF_Growth,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())
        # print(ac_lob_total_id)

        # create Home LOB report
        Line_of_Business = "Home"
        name_string = Name +  ' - Home'
        YTD_PIF = currency_format(str(df.loc[1][8]))
        YTD_PIF_Growth = currency_format(str(df.loc[5][8]))
        YTD_DWP = currency_format(str(df.loc[27][8]))
        YTD_Loss_Ratio = currency_format(str(df.loc[31][8]))
        YTD_Quotes = currency_format(str(df.loc[18][8]))
        Prior_YTD_PIF = currency_format(str(df.loc[1][9]))
        Prior_YTD_Quotes = currency_format(str(df.loc[18][9]))
        Prior_YTD_DWP = currency_format(str(df.loc[27][9]))
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_PIF_Growth": YTD_PIF_Growth,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())

        # create OPL LOB report
        Line_of_Business = "OPL"
        name_string = Name + ' - OPL'
        YTD_PIF = currency_format(str(df.loc[2][8]))
        YTD_PIF_Growth = currency_format(str(df.loc[6][8]))
        YTD_DWP = currency_format(str(df.loc[28][8]))
        YTD_Loss_Ratio = currency_format(str(df.loc[32][8]))
        YTD_Quotes = currency_format(str(df.loc[19][8]))
        Prior_YTD_PIF = currency_format(str(df.loc[2][9]))
        Prior_YTD_Quotes = currency_format(str(df.loc[19][9]))
        Prior_YTD_DWP = currency_format(str(df.loc[28][9]))
        request_body = {
                "data": [{
                        "Name": name_string,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Carrier_Report": carrier_report_id,
                        'Line_of_Business': Line_of_Business,
                        "Report_Date": Report_Date,
                        "YTD_PIF": YTD_PIF,
                        "YTD_PIF_Growth": YTD_PIF_Growth,
                        "YTD_DWP": YTD_DWP,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_PIF": Prior_YTD_PIF,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "Prior_YTD_DWP": Prior_YTD_DWP
                }]
            }
        ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        ac_lob_total_id = (ac_lob.json())


    elif "Agency Selection" in filename:
        Name = 'Travelers (Agency Selection) - ' + word_date

        # In lambda, modify it to append every table to the df in a loop without manually stating which file
        table1 = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table1.csv")
        df = table1
        table_count = 1
        while table_count < 50:
            try:
                table_count += 1 
                table = pd.read_csv('s3://textractkvtable-us-east-1-907387566050/tables/' + foldername + "/table"+str(table_count)+".csv")
                df = df.append(table, ignore_index=True)
            except:
                pass

        # create a carrrier report
        request_body = {
                "data": [{
                        "Name": Name,
                        "Carrier": "Travelers",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                }]
            }
        carrier_report = requests.post(url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = carrier_report.json()
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])
    
        new_agency_code_count = 0
        member_consolidated_count = 0

        count = 0

        # Create member records
        for i, row in df.iterrows():
            account_id = ''
            contact_id = ''
            agency_code_id = ''
            member_consolidated_id = ''
            name_string = ''
            count += 1
            print(count)
            if row["Region"] != " ": 
                agency_code = row["Prime Code"]
                agency_name = row["Agency Name"]
                name_string = agency_name + " - " + Name
                YTD_DWP = currency_format(str(row['Written Premium']))
                YTD_PIF = currency_format(str(row['PIF Count']))
                YTD_Loss_Ratio = currency_format(str(row['Loss Ratio']))
                YTD_Quotes = currency_format(str(row["Quotes"]))
                
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
                    if(account_id == ''):
                        # if no account was found then we need the account id of the unassigned account. Find it in zoho, because this assignment below of 0 will give you an error.

                        account_id = 5187612000000754742

                        # we found an account id match so now we just need to create our agency code record
                        request_body = {
                            "data": [
                                {
                                    "Account": account_id,
                                    "Name": agency_code,
                                    "Carrier": "Travelers",
                                    "UAC_Name": agency_name,
                                    "Contact": contact_id
                                    # add additional fields here
                                }
                            ]
                        }
                        # this is our API call to the CRM API to post a record in the Agency Code Module.
                        agency_code_response = requests.post(
                            url=agency_code_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

                        if(agency_code_response.json()['data'][0]['status'] == "error"):

                            print("  ")
                        
                            print("  ")
                            return
                        
                        # if it returns a successful response, then the created agency code record id is stored into the corresponding variable

                        agency_code_id = agency_code_response.json()[
                            'data'][0]['details']['id']
                        new_agency_code_count += 1
                
                request_body = {
                        "data": [{
                                "Name": name_string,
                                "Carrier": "Travelers",
                                "Report_Type": ['YTD'],
                                "Report_Date": Report_Date,
                                "Account": account_id,
                                "Agency_Code": agency_code_id,
                                "Carrier_Report": carrier_report_id,
                                "YTD_DWP": YTD_DWP,
                                "YTD_PIF": YTD_PIF,
                                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                                "YTD_Quotes": YTD_Quotes
                        }]
                    }
                consolidated_response = requests.post(
                url=consolidated_url,
                headers=headers,
                data=json.dumps(request_body).encode('utf-8'))
                member_consolidated_count += 1

            if (consolidated_response.json()['data'][0]['status'] == "error"):
                print("  ")
                print(request_body)
                print(consolidated_response.json())
                print("  ")
                return

        
        print('Consolidated records created: ' + str(member_consolidated_count))
        print('Agency Codes created: ' + str(new_agency_code_count))
    return



def currency_format(num):
    num = num.replace('$','')
    num = num.replace(',','')
    num = num.replace('%','')
    num = num.replace('(','')
    num = num.replace(')','')
    return num

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

main()