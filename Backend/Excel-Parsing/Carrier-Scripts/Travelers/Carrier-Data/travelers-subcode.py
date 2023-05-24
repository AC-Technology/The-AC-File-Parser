from cmath import nan
from tkinter import PAGES
from unicodedata import name
from numpy import NaN
import tabula
import os
import json
import base64
import requests
from io import BytesIO

def main():
    filename = "Travelers(Subcode) - March 2022.xlsx"
    dot = filename.index(".")
    filename = filename[:dot]

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
        try:
            for i in range(len(agency_get_response.json()['data'])):
                agency_code_records.append(agency_get_response.json()['data'][i])
            if(not agency_get_response.json()['info']['more_records']):
                more_records = False
        except requests.exceptions.JSONDecodeError:
            print("No agency codes found")
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
 
  

    member_consolidated_count = 0
    new_agency_code_count = 0

    count = 0


    #Report Date
    words= filename.split()
    list_words=[words[index]+' '+words[index+1] for index in range(len(words)-1)]


    months = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    days = dict(Jan=31,Feb=28,Mar=31,Apr=29,May=31,Jun=30,Jul=31,Aug=31,Sep=30,Oct=31,Nov=30,Dec=30)

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

    carrier_url = 'https://www.zohoapis.com/crm/v2/Carrier_Reports'
    if "Subcode" in filename:
        Name = "Travelers(Subcode) - " + word_date
        # First time read in to read the Prior DWP Total properly on the last page
        df = tabula.read_pdf("Travelers/Carrier-Data/Travelers PL - Agency Collective Sub-Code Details March 2022 Master (1).pdf", pages = '11',area=[110,15,560,571], lattice = True)
        # print(df)
        YTD_Quotes = format_currency(df[-1]["Quotes"].iloc[-1])
        YTD_Submits = format_currency(df[-1]["BNB"].iloc[-1])
        YTD_PIF = format_currency(df[-1]["Policies\rin Force"].iloc[-1])
        YTD_PIF_Growth = format_percentage(df[-1]["PIF\rGrowth"].iloc[-1])
        YTD_Retention = format_percentage(df[-1]["Retention"].iloc[-1])
        YTD_Loss_Ratio = format_percentage(df[-1]["Loss\rRatio"].iloc[-1])
        Prior_YTD_Submits = format_currency(df[-1]["BNB.1"].iloc[-1])
        Prior_YTD_PIF = format_currency(df[-1]["Policies\rin Force.1"].iloc[-1])
        Prior_YTD_DWP = format_currency(df[-1]["Written\rPremium.1"].iloc[-1])
        # Second time read in to read the DWP Total properly on the last page
        df = tabula.read_pdf("Travelers/Carrier-Data/Travelers PL - Agency Collective Sub-Code Details March 2022 Master (1).pdf", pages = '11',area=[110,15,560,460], lattice = True)
        YTD_DWP = format_currency(df[-1]["Written\rPremium"].iloc[-1])

        # Create Carrier Reprot
        request_body = {
            "data": [{
                    "Name": Name,
                    "Carrier": "Travelers",
                    "Report_Type": ['YTD'],
                    "Report_Date": Report_Date,
                    "YTD_Quotes": YTD_Quotes,
                    "YTD_Submits": YTD_Submits,
                    "YTD_PIF": YTD_PIF,
                    "YTD_PIF_Growth": YTD_PIF_Growth,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
                    "YTD_DWP": YTD_DWP,
                    "Prior_YTD_Submits": Prior_YTD_Submits,
                    "Prior_YTD_PIF": Prior_YTD_PIF,
                    "Prior_YTD_DWP": Prior_YTD_DWP,
                    "YTD_Loss_Ratio": YTD_Loss_Ratio,
            }]
        }
        carrier_report = requests.post(
                url=carrier_url, headers=headers, data=json.dumps(request_body, default=str).encode('utf-8'))
        carrier_report_id = (carrier_report.json())
        carrier_report_id = (carrier_report_id["data"][0]["details"]['id'])



        # Third time read in to iterate through whole pdf        
        df = tabula.read_pdf("Travelers/Carrier-Data/Travelers PL - Agency Collective Sub-Code Details March 2022 Master (1).pdf", pages = 'all',area=[110,15,560,753], lattice = True)
        iterations = 0
        for count, i in enumerate(df):
            for x, row in i.iterrows():
                if df[count]["Sub-Code"][x] != "Total":
                    iterations += 1
                    print(iterations)
                    df[count] = df[count].replace('\r',' ', regex=True)
                    df[count] = df[count].fillna("")
                    # print(row)
                    # reset variables
                    account_id = ''
                    contact_id = ''
                    agency_code_id = ''
                    uac_number = ''  # if needed
                    member_consolidated_id = ''
                    agency_code = df[count]["Sub-Code"][x]
                    agency_name = df[count]["Agency Name"][x].replace('\\',' ')
                    print(agency_name)
                    name_string = agency_name + " - Travelers - " + word_date
                    # print(Agency_Name)
                    YTD_Quotes = format_currency(df[count]["Quotes"][x])
                    YTD_Submits = format_currency(df[count]["BNB"][x])
                    YTD_PIF = format_currency(df[count]["Policies\rin Force"][x])
                    YTD_PIF_Growth = format_percentage(df[count]["PIF\rGrowth"][x])
                    YTD_Retention = format_percentage(df[count]["Retention"][x])
                    YTD_Loss_Ratio = format_percentage(df[count]["Loss\rRatio"][x])
                    YTD_DWP = format_currency(df[count]["Written\rPremium"][x])
                    Prior_YTD_Submits = format_currency(df[count]["BNB.1"][x])
                    Prior_YTD_PIF = format_currency(df[count]["Policies\rin Force.1"][x])
                    Prior_YTD_DWP = format_currency(df[count]["Written\rPremium.1"][x])
                    
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
                            account_id = 5187612000000560802
                            # we found an account id match so now we just need to create our agency code record.

                        request_body = {
                            "data": [{
                                "Account": account_id,
                                "Name": agency_code,
                                "UAC": uac_number,
                                "Carrier": "Travelers",
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
                            # If it returns a successful response, then the created agency code record id is stored into the corresponding variable

                        new_agency_code_count += 1

                    request_body = {
                        "data": [{
                                'Account': account_id,
                                'Contact': contact_id,
                                'Agency_Code': agency_code_id,
                                'Uniform_Agency_Code': uac_number,
                                "Carrier_Report": carrier_report_id,
                                "Name": name_string,
                                "Carrier": "Travelers",
                                "Report_Type": ['YTD'],
                                "Report_Date": Report_Date,
                                "YTD_Quotes": YTD_Quotes,
                                "YTD_Submits": YTD_Submits,
                                "YTD_PIF": YTD_PIF,
                                "YTD_PIF_Growth": YTD_PIF_Growth,
                                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                                "YTD_DWP": YTD_DWP,
                                "Prior_YTD_Submits": Prior_YTD_Submits,
                                "Prior_YTD_PIF": Prior_YTD_PIF,
                                "Prior_YTD_DWP": Prior_YTD_DWP,
                                "YTD_Loss_Ratio": YTD_Loss_Ratio,
                        }]
                    }

                    # Now we make our member consolidated record.
                    consolidated_response = requests.post(
                        url=consolidated_url, headers=headers, data=json.dumps(request_body).encode('utf-8'))

                    if (consolidated_response.json()['data'][0]['status'] == "error"):
                        print("  ")
                        print(request_body)
                        print(consolidated_response.json())
                        print("  ")
                        return

                    member_consolidated_count += 1


        ## You're done! The only code you may need to add is for member line of business data specific data, which is seen in the more complicated reports and must also be inserted into the system. Use the template to write a new section that correctly maps the Lob Data. Before you do this section, please call me over.
    print('Consolidated records created: ' + str(member_consolidated_count))
    print('Agency Codes created: ' + str(new_agency_code_count))
    return



                        

def format_percentage(num):
    num = str(num)
    if "%" in num:
        num = num.replace('%',"")
    if "(" in num:
        num = num.replace('(',"")
        num = num.replace(')',"")
        num = "-" + num
    return num


def format_currency(num):
    num = str(num)
    num = num.replace(',', '')

    if "$" in num:
        num = num.replace('$', '')

    if "(" in num:
        num = num.replace('(',"")
        num = num.replace(')',"")
        num = "-" + num

    num = int(num)
    return num

main()