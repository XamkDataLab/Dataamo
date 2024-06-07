import os
import re
import hashlib
import numpy as np
import progressbar
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers

# NOTE: some module_wide variables here

# Load the environment variables from the .env file
load_dotenv(".env")
CUSTOMER_NAME = os.getenv("CUSTOMER_NAME")
API_KEY = os.getenv("API_KEY")

class YtjClient:
    def __init__(self):
        self.client = Client('https_api_tietopalvelu_ytj_fi_yritystiedot.wsdl')

    # Generates a current timestamp and calculates a valid YTJ API token ("tunniste", this is not the API key)
    #
    def _get_timestamp_and_token(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        input_string = f"{CUSTOMER_NAME}{API_KEY}{timestamp}"
        sha1_token = hashlib.sha1(input_string.encode()).hexdigest()
        return timestamp, sha1_token

    def get_multiple(self, bids):
        out = []
        maxsize = max(5, len(bids) // 100)  # Ensure maxsize is at least 5
        batches = np.array_split(bids, np.ceil(len(bids) / maxsize))

        with progressbar.ProgressBar(max_value=len(bids)) as progress:
            for i, batch in enumerate(batches):
                timestamp, token = self._get_timestamp_and_token()
                params = {
                    "ytunnus": ";".join(batch),
                    "kieli": "fi",
                    "asiakastunnus": CUSTOMER_NAME,
                    "aikaleima": timestamp,
                    "tarkiste": token,
                    "tiketti": ""
                }
                out += self.client.service.wmYritysTiedotMassahaku(**params)
                progress.update((i + 1) * maxsize)  # Update to the next batch size

        return out

    def search(self, str_="", bid=""):
        timestamp, token = self._get_timestamp_and_token()
        params = {
            "hakusana": str_,
            "ytunnus": bid,
            "yritysmuoto": "",
            "sanahaku": "false",
            "voimassaolevat": "true",
            "kieli": "fi",
            "asiakastunnus": "xamk",
            "aikaleima": timestamp,
            "tarkiste": token,
            "tiketti": ""
        }
        return self.client.service.wmYritysHaku(**params)

    def parse_company(self, company, verbose = False):
        
        data = helpers.serialize_object(company)

        if data is None or data['YritysTunnus'] is None or data['YritysTunnus']['YTunnus'] is None:
            return None

        businessid = None
        zipcode = None
        businessline = None
        format = None
        registration = None
        status = "Aktiivinen"
        name = '[tyhjä]' # 'yritys' column is set to NOT NULL

        businessid = data['YritysTunnus']['YTunnus']

        if data['Toiminimi']:
            name = data['Toiminimi']['Toiminimi']
        
        if name == '[tyhjä]' and data['YrityksenHenkilo']:
            name = data['YrityksenHenkilo']['Nimi']

        if data['YritysTunnus']['YrityksenLopettamisenSyy']:
            status = data['YritysTunnus']['YrityksenLopettamisenSyy']

        if data['Toimiala']['Seloste']:
            businessline = data['Toimiala']['Seloste'] + " (" + data['Toimiala']['Koodi'] + ")"

        if data['YrityksenPostiOsoite']:
            zipcode = data['YrityksenPostiOsoite']['Postinumero']

        if zipcode == None and data['YrityksenKayntiOsoite']:
            zipcode = data['YrityksenKayntiOsoite']['Postinumero']

        if data['Yritysmuoto']:
            format = data['Yritysmuoto']['Seloste']

        if data['YritysTunnus']['Alkupvm']:
            # We are getting dates as "dd.mm.yyyy" but prefer "yyyy-mm-dd"
            d = data['YritysTunnus']['Alkupvm']
            registration = f"{d[6:]}-{d[3:5]}-{d[:2]}"

        if verbose:
            print(businessid + ":", name, format, status, businessline, zipcode)

        return [businessid, name, format, businessline, zipcode, registration, status]

    def upsert_company(self, connection, columns, data):
        cursor = connection.cursor()
        update = ', '.join([f'{column} = ?' for column in columns])
        column_names = ', '.join(columns)
        placeholders = ', '.join(["?"] * len(columns))

        update_sql = f"UPDATE yritykset SET {update} WHERE y_tunnus = ?"
        insert_sql = f"INSERT INTO yritykset ({column_names}) VALUES ({placeholders})"
        
        # Update needs the "y_tunnus" twice, second time at the end
        cursor.execute(update_sql, data + [data[0]])

        # If no rows where updated, we need to insert this row
        if(cursor.rowcount) == 0:
            cursor.execute(insert_sql, data)
        
        connection.commit()

    # Define the upsert_company_batch function to handle batch upsert
    def upsert_company_batch(self, connection, columns, batch_data):
        cursor = connection.cursor()
        update = ', '.join([f'{column} = ?' for column in columns])
        column_names = ', '.join(columns)
        placeholders = ', '.join(["?"] * len(columns))

        update_sql = f"UPDATE yritykset SET {update} WHERE y_tunnus = ?"
        insert_sql = f"INSERT INTO yritykset ({column_names}) VALUES ({placeholders})"

        # Execute batch upsert using executemany
        for data in batch_data:
            # Update needs the "y_tunnus" twice, second time at the end
            cursor.execute(update_sql, data + [data[0]])

            # If no rows were updated, insert the row
            if cursor.rowcount == 0:
                cursor.execute(insert_sql, data)
                
    # In the late 1970's a few business ids on the range from 9000000 upwards where given out,
    # so the latest business id is the max id below that range.
    #
    def get_latest_bid(self, connection):
        cursor = connection.cursor()
        sql = "SELECT MAX(y_tunnus) FROM yritykset WHERE y_tunnus < '9000000-0'"
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0]

    def mark_empty_bid(self, connection, bid):
        cursor = connection.cursor()
        insert_sql = f'INSERT INTO unused_businessids ("bid", "checked") VALUES (?, ?)'
        current = datetime.now()
        cursor.execute(insert_sql, bid, current)    

    # Because "0 == False" equals True, we use "-1" as "invalid"
    #
    def bid_checksum(self, bid):
        bid = str(bid).zfill(7)
        base = [int(x) for x in list(bid[:7])]
        mod = sum(np.multiply(base, [7,9,10,5,8,4,2])) % 11
        if mod == 1:
            return -1
        return 0 if (mod == 0) else (11 - mod)

    def check_bid(self, bid):
        bid = str(bid)
        bid_pattern = r'\d{7}-\d'
        if not re.match(bid_pattern, bid):
            return False
        check = int(bid[-1:])
        checksum = self.bid_checksum(bid)
        return checksum == check

    def generate_bids(self, start, count):
        # If an existing business id is given, fetch the number portion of it.
        start = int(str(start).partition('-')[0])
        bids = []
        while(count):
            sbid = str(start).zfill(7)
            checksum = self.bid_checksum(sbid)
            start += 1
            # Every 11th business id is skipped, don't count the skipped ones
            if checksum > -1:
                fullbid = str(sbid) + "-" + str(checksum)
                bids.append(fullbid)
                count -= 1
        return bids

    def load_bids_from_file(self, file_name):
        bids = []
        with open(file_name, 'r', encoding='utf-8') as file:
            for line in file:
                # Some table exports have extra characters at the end and at the beginning of the line
                bid_pattern = r'(\d{7}-\d)'
                match = re.search(bid_pattern, line)
                if match:
                    bid = match.group(1)
                    print(type(bid))
                    if self.check_bid(bid):
                        bids.append(bid)
        return bids
    
    def load_new_companies(self, next_bid, count):
        bids = self.generate_bids(next_bid, count)
        companies = self.get_multiple(bids)
        return companies

    def store_companies_to_db(self, companies, columns, connection):
        progress = progressbar.ProgressBar(max_value=len(companies))
        progress.update(0)

        for i, company in enumerate(companies):
            company_data = self.parse_company(company)
            if company_data:
                # Add the column for the "last checked" date
                company_data.append(datetime.today().strftime('%Y-%m-%d'))
                self.upsert_company(connection, columns, company_data)
            progress.update(i)

        # Finish the progress bar
        progress.finish()