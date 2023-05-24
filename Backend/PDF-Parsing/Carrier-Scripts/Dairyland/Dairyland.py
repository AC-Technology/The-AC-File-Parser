from unicodedata import name
import pandas as pd
import requests
import json
# import boto3
import io

def main():
    filename = 'Dairyland(Monthly Recap) - December 2021'

    # print(event)
    # key = event['params']['querystring']['key']
    # print(key)
    # filename = event['params']['querystring']['filename']
    # dot = filename.index(".")
    # filename = filename[:dot]

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
        search_params = {'criteria': 'Carrier:equals:Dairyland',
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


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)
   
    #Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]

    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=30,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

    word_date = list_words[3]
    space = list_words[3].index(" ")
    month = list_words[3][:3]
    year = list_words[3][space:]
    day = days[month]
    month = months[month]
    # print(month)

    if month < 10:
        Report_Date = str(year)  + "-" + "0"+str(month) + "-" + str(day)
    else:
        Report_Date = str(year) + "-" + str(month) + "-" + str(day)

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    Name = "Dairyland(Monthly Recap) - " + word_date
     # In lambda, modify it to append every table to the df in a loop without manually stating which file
    # table1 = pd.read_csv("May(Monthly Recap)//table_21c2f7d6146b4d9ab925620c0bd7fb3c_page_1.csv")
    # table2 = pd.read_csv("May(Monthly Recap)//table_001d2b3081a64f37bdf0f8e0aef19bd6_page_1.csv")
    # table3 = pd.read_csv("May(Monthly Recap)//table_ded772c73ea647768ee755edd8918919_page_1.csv")
    # table4 = pd.read_csv("May(Monthly Recap)//table_7aa4b163569a4a7ba0de15f9e500ed58_page_2.csv")
    # table5 = pd.read_csv("May(Monthly Recap)//table_3a6a6637e347429aa0f9357e25c9153b_page_3.csv")
    # table6 = pd.read_csv("May(Monthly Recap)//table_7c15af58f3ef40c5a9ee8ebc43bac869_page_4.csv")
    # table7 = pd.read_csv("May(Monthly Recap)//table_87e33d0abca94c75a4361af49a8b6807_page_5.csv")
    # table8 = pd.read_csv("May(Monthly Recap)//table_9b22abe0d7904e8099d8df68a8df45cd_page_6.csv")
    # table9 = pd.read_csv("May(Monthly Recap)//table_a006256352284bb8abebf4b8a5feeac1_page_7.csv")
    # table10 = pd.read_csv("May(Monthly Recap)//table_5f23e2ceee594c96bcb16a09eec4c94a_page_8.csv")

    table1 = pd.read_csv("December(Monthly Recap)//table_ba33a5ee898940a0a85fd1838c54bf26_page_1.csv")
    table2 = pd.read_csv("December(Monthly Recap)//table_3245e86bd06143c2a7f6bbf32dfb4d12_page_1.csv")
    table3 = pd.read_csv("December(Monthly Recap)//table_dfc57755f22647bf98ca33c95570b69b_page_1.csv")
    table4 = pd.read_csv("December(Monthly Recap)//table_7a7e730ba1194654b9a50d87bcfc4ef3_page_2.csv")
    table5 = pd.read_csv("December(Monthly Recap)//table_1b236e8403b946bda6080f0a1099d165_page_3.csv")
    table6 = pd.read_csv("December(Monthly Recap)//table_30b70e7372d244018bd9bb56c702db46_page_4.csv")
    table7 = pd.read_csv("December(Monthly Recap)//table_0f9b8fc326f74252b1a01c18f1a6a9fb_page_5.csv")
    table8 = pd.read_csv("December(Monthly Recap)//table_b86f81376a774a488d03332b3289e4b0_page_6.csv")
    table9 = pd.read_csv("December(Monthly Recap)//table_d3dc1603cc344b6aa32f3387ff319a0a_page_7.csv")
    table10 = pd.read_csv("December(Monthly Recap)//table_08038aae9bca4541bdc6e7b7b6c09808_page_8.csv")


    YTD_DWP = currency_format(table4['Date Range'][23])
    Prior_YTD_DWP = currency_format(table4[' .3'][23])
    Earned_Premium_YTD = currency_format(table4['Date Range'][24]).replace('.','')
    Incurred_Loss_YTD = currency_format(table4['Date Range'][25]).replace('.','')
    YTD_Loss_Ratio = currency_format(table4['Date Range'][26])
    YTD_Quotes = currency_format(table4['Date Range'][20])
    Prior_YTD_Quotes = currency_format(table4[' .3'][20])
    YTD_NB_PIF = currency_format(table4['Date Range'][21])
    PYTD_YTD_NB_PIF = currency_format(table4[' .3'][21])
    YTD_Hit_Ratio = currency_format(table4['Date Range'][22])
    YTD_PIF = currency_format(table4['Date Range'][18])
    Prior_YTD_PIF = currency_format(table4[' .3'][18])
    
    request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Dairyland",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "Incurred_Loss_YTD": Incurred_Loss_YTD,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "YTD_Quotes": YTD_Quotes,
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "PYTD_YTD_NB_PIF": PYTD_YTD_NB_PIF,
                    "YTD_Hit_Ratio": YTD_Hit_Ratio,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF
            }]
        }
    # print(request_body)
    carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    carrier_report_id = carrier_report.json()
    # print(carrier_report_id)
    carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])

    # Create Specialty Auto LOB Report
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = "Specialty Auto"
    name_string = Name + " - " + Line_of_Business
    YTD_DWP = currency_format(table4['Date Range'][5])
    Prior_YTD_DWP = currency_format(table4[' .3'][5])
    Earned_Premium_YTD = currency_format(table4['Date Range'][6]).replace('.','')
    Incurred_Loss_YTD = currency_format(table4['Date Range'][7]).replace('.','')
    YTD_Loss_Ratio = currency_format(table4['Date Range'][8])
    YTD_Quotes = currency_format(table4['Date Range'][2]).replace('.','')
    Prior_YTD_Quotes = currency_format(table4[' .3'][2])
    YTD_NB_PIF = currency_format(table4['Date Range'][3])
    PYTD_YTD_NB_PIF = currency_format(table4[' .3'][3])
    YTD_Hit_Ratio = currency_format(table4['Date Range'][4])
    YTD_PIF = currency_format(table4['Date Range'][9])
    Prior_YTD_PIF = currency_format(table4[' .3'][9])

    request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier": "Dairyland",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Carrier_Report": carrier_report_id,
                    "Line_of_Business": Line_of_Business,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "Incurred_Loss_YTD": Incurred_Loss_YTD,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "YTD_Quotes": YTD_Quotes,
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "PYTD_YTD_NB_PIF": PYTD_YTD_NB_PIF,
                    "YTD_Hit_Ratio": YTD_Hit_Ratio,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF
            }]
        }
    ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    # print(ac_lob_total_id)

    # Create Motorcycle Report
    lob_total_url = 'https://www.zohoapis.com/crm/v2/AC_LOB_Data_Carrier'
    Line_of_Business = "Motorcycle" 
    name_string = Name + " - " + Line_of_Business
    YTD_DWP = currency_format(table4['Date Range'][14])
    Prior_YTD_DWP = currency_format(table4[' .3'][14])
    Earned_Premium_YTD = currency_format(table4['Date Range'][15]).replace('.','')
    Incurred_Loss_YTD = currency_format(table4['Date Range'][16]).replace('.','').replace('[','').replace(']','')
    YTD_Loss_Ratio = currency_format(table4['Date Range'][17])
    YTD_Quotes = currency_format(table4['Date Range'][11]).replace('.',',')
    Prior_YTD_Quotes = currency_format(table4[' .3'][11])
    YTD_NB_PIF = currency_format(table4['Date Range'][12])
    PYTD_YTD_NB_PIF = currency_format(table4[' .3'][12])
    YTD_Hit_Ratio = currency_format(table4['Date Range'][13])
    YTD_PIF = currency_format(table4['Date Range'][18])
    Prior_YTD_PIF = currency_format(table4[' .3'][18])

    request_body = {
            "data": [{
                    "Name": name_string,
                    "Carrier": "Dairyland",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "Carrier_Report": carrier_report_id,
                    "Line_of_Business": Line_of_Business,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "Earned_Premium_YTD": Earned_Premium_YTD,
                    "Incurred_Loss_YTD": Incurred_Loss_YTD,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "YTD_Quotes": YTD_Quotes,
                    "Prior_YTD_Quotes": Prior_YTD_Quotes,
                    "YTD_NB_PIF": YTD_NB_PIF,
                    "PYTD_YTD_NB_PIF": PYTD_YTD_NB_PIF,
                    "YTD_Hit_Ratio": YTD_Hit_Ratio,
                    "YTD_PIF": YTD_PIF,
                    "Prior_YTD_PIF": Prior_YTD_PIF
            }]
        }
    ac_lob = requests.post(
        url=lob_total_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
    ac_lob_total_id = (ac_lob.json())
    # print(request_body)
    # print(ac_lob_total_id)

    df = table7
    new_df = (table8.T.reset_index().T.reset_index(drop=True)
            .set_axis([f'Q1.{i+1}' for i in range(table8.shape[1])], axis=1))
    new_df.columns = table7.columns
    # print(new_df)
    df = df.append(new_df, ignore_index=True)
    # print(df.columns)
    
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    # print(df)

    new_agency_code_count = 0
    member_consolidated_count = 0
    # Create Agency Reports
    for i,row in df.iterrows():
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        member_consolidated_id = ''
        name_string = ''
        if i != 0:
            print(i)
            agency_code = row[" "]
            agency_name = row[" .1"]
            if agency_name == ' ':
                agency_name = "Unnamed"
            name_string = agency_name + " - " + Name
            YTD_DWP = currency_format(row[' .5'])
            if month == 12:
                Prior_YTD_DWP = currency_format(row['Written Premium'])
            else:
                Prior_YTD_DWP = currency_format(row['Written Premi'])
            Earned_Premium_YTD = currency_format(row['Earned Premium'])
            Incurred_Loss_YTD = currency_format(row['Losses'])
            YTD_Loss_Ratio = currency_format(row['Loss Ratio'])
            YTD_Quotes = currency_format(row[' .8'])
            Prior_YTD_Quotes = currency_format(row['Quotes'])
            if Prior_YTD_Quotes == 'n':
                Prior_YTD_Quotes = 0
            YTD_NB_PIF = currency_format(row['New'])
            PYTD_YTD_NB_PIF = currency_format(row['Policies'])
            if month == 12:
                YTD_Hit_Ratio = currency_format(row['Close Rati'])
            else:
                YTD_Hit_Ratio = currency_format(row['Close Ratio'])
            YTD_PIF = currency_format(row['Inforce Policies'])
            Prior_YTD_PIF = currency_format(row[' .10'])
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
                                "Carrier": "Dairyland",
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
                        "Carrier": "Dairyland",
                        "Report_Type": ['YTD'],
                        "Report_Date": Report_Date,
                        "Carrier_Report": carrier_report_id,
                        "Account": account_id,
                        "Agency_Code": agency_code_id,
                        "YTD_DWP": YTD_DWP,
                        "Prior_YTD_DWP": Prior_YTD_DWP,
                        "Earned_Premium_YTD": Earned_Premium_YTD,
                        "Incurred_Loss_YTD": Incurred_Loss_YTD,
                        "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        "YTD_Quotes": YTD_Quotes,
                        "Prior_YTD_Quotes": Prior_YTD_Quotes,
                        "YTD_NB_PIF": YTD_NB_PIF,
                        "PYTD_YTD_NB_PIF": PYTD_YTD_NB_PIF,
                        "YTD_Hit_Ratio": YTD_Hit_Ratio,
                        "YTD_PIF": YTD_PIF,
                        "Prior_YTD_PIF": Prior_YTD_PIF
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


def currency_format(num):
    num = str(num)
    if 'o' in num:
        num = '0'
    if 'sn' in num:
        num = '0'
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