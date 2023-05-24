from cmath import nan
from email import header
from operator import itemgetter
from threading import current_thread

from numpy import NaN, rec
import pandas as pd
from difflib import SequenceMatcher
import requests
import json
import tabula
from jdk4py import JAVA, JAVA_HOME, JAVA_VERSION
from tabula import read_pdf



# GLOBAL VARIABLES
# Carrier Report ID
# This will come from the 'Clearcover - April 2022' AC Carrier Report URL
carrier_report_id = 5187612000002297324

# These are the client id, secret, and the refresh token you will be using to make API calls to the CRM API.
# They are given to us by zoho and are unique to every CRM.
# The way authentication with zoho is laid out is on the board behind Tran's desk.

client_id = '1000.G01B0UNFGTFK5HSMYSO8QCBP5R5LKX'
client_secret = '262eec9c5c47b597f5666fc78ee6369912fde29bfc'
refresh_token = '1000.b6365733e341a5c2c8617643aaeecd99.017bf902178c60dd7f76448770b2deca'

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


def main():
    # This part of our code retrieves the access token
    # using the built-in request library for making http requests, we make a post request to the auth url to retrieve an access token.
    auth_response = requests.post(url=auth_url)

    # Turn the response into readable json and store the access token in a variable
    access_token = auth_response.json()['access_token']

    # this authorization header will be used to make API calls. This is specified in the Zoho CRM documentation.
    # https://www.zoho.com/crm/developer/docs/api/v2/insert-records.html
    # it would be helpful to have this open in a tab.

    headers = {'Authorization': 'Zoho-oauthtoken ' + access_token}

    # Read pdf into list of DataFrame                                            # The Area format is[[top,left,top+height,left+width]] 
    Travelers = tabula.read_pdf("Travelers\Carrier-Data\Travelers-March-2022.pdf", pages='3', multiple_tables=True, area=[['124.2','18','553.1','763,61']])
    
    # for i in range(len(Travelers)):
    #     Travelers[i].to_csv(f"Table_{i}.csv")

    print(Travelers)
    # Zoho Account Data
    zoho_account_data = pd.read_csv('Zoho-Data\Accounts_001.csv')


# y1 = top
# x1 = left
# y2 = top + height
# x2 = left + width
# Order
# y1,x1,y2,x2


main()