import pandas as pd
carrier_data = pd.read_excel('Main-Street-America\Carrier-Data\Main Street America YTD by Locations thru April 2022.xlsx', engine='openpyxl', sheet_name="Agent Summary")
for i, row in carrier_data[21:-1].iterrows():
        # loop variables. These are reset every new row and must be filled in.
        # if we do not reset them every loop, then we run the risk of assigning the wrong records together since an id variable could contain a previous found id.
        account_id = ''
        contact_id = ''
        agency_code_id = ''
        uac_number = ''  # if needed
        member_consolidated_id = ''

        # These are the variables that store the value of the corresponding cell.
        # Agency Name
        agency_name = str(row['Unnamed: 1']).strip()
        print(agency_name)
        # Agency Code
        agency_code = str(row["Unnamed: 0"]).strip()
        print(agency_code)
        name_string = agency_name + " - Main Street America - April 2022"