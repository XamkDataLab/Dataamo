import os
import re
import hashlib
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers
from sqlalchemy.exc import SQLAlchemyError

# Load the environment variables from the .env file
load_dotenv(".env")
CUSTOMER_NAME = os.getenv("CUSTOMER_NAME")
API_KEY = os.getenv("API_KEY")

class YtjClient:
    def __init__(self, wsdl_url='https_api_tietopalvelu_ytj_fi_yritystiedot.wsdl'):
        self.client = Client(wsdl_url)
        self.db_client = None  # Initially, no database client is set

    def set_database(self, db_client):
        """Set the database client."""
        self.db_client = db_client

    def _get_timestamp_and_token(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        input_string = f"{CUSTOMER_NAME}{API_KEY}{timestamp}"
        sha1_token = hashlib.sha1(input_string.encode()).hexdigest()
        return timestamp, sha1_token

    def get_multiple(self, bids, progbar):
        out = []
        maxsize = max(5, len(bids) // 100)
        batches = np.array_split(bids, np.ceil(len(bids) / maxsize))
        bartext = "Reading new company data..."

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
            progbar.progress((i / len(batches)), f"{bartext} ({i*maxsize} of {len(bids)})")
        progbar.progress(100, bartext)

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

    def parse_company(self, company, verbose=False):
        data = helpers.serialize_object(company)

        if data is None or data['YritysTunnus'] is None or data['YritysTunnus']['YTunnus'] is None:
            return None

        businessid = data['YritysTunnus']['YTunnus']
        name = data['Toiminimi']['Toiminimi'] if data['Toiminimi'] else '[tyhjä]'
        if name == '[tyhjä]' and data.get('YrityksenHenkilo'):
            name = data['YrityksenHenkilo']['Nimi']

        status = data['YritysTunnus'].get('YrityksenLopettamisenSyy', "Aktiivinen")
        businessline = (f"{data['Toimiala']['Seloste']} ({data['Toimiala']['Koodi']})" 
                        if data.get('Toimiala') else None)
        zipcode = (data['YrityksenPostiOsoite']['Postinumero'] 
                   if data.get('YrityksenPostiOsoite') else None)
        if not zipcode and data.get('YrityksenKayntiOsoite'):
            zipcode = data['YrityksenKayntiOsoite']['Postinumero']

        format = data['Yritysmuoto']['Seloste'] if data.get('Yritysmuoto') else None
        registration = None
        if data['YritysTunnus'].get('Alkupvm'):
            d = data['YritysTunnus']['Alkupvm']
            registration = f"{d[6:]}-{d[3:5]}-{d[:2]}"

        if verbose:
            print(f"{businessid}: {name}, {format}, {status}, {businessline}, {zipcode}")

        return [businessid, name, format, businessline, zipcode, registration, status]

    def upsert_company(self, columns, data):
        if self.db_client is None:
            raise DatabaseError("No database client set.")
        with self.db_client as db:
            try:
                update = ', '.join([f'{column} = :{column}' for column in columns])
                column_names = ', '.join(columns)
                placeholders = ', '.join([f":{column}" for column in columns])
                
                update_sql = f"UPDATE companies SET {update} WHERE business_id = :business_id"
                insert_sql = f"INSERT INTO companies ({column_names}) VALUES ({placeholders})"
                
                params = {**dict(zip(columns, data)), "business_id": data[0]}
                db._session.execute(text(update_sql), params)
                
                if db._session.execute("SELECT @@ROWCOUNT").scalar() == 0:
                    db._session.execute(text(insert_sql), params)
                
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error upserting company: {e}")

    def upsert_company_batch(self, columns, batch_data):
        if self.db_client is None:
            raise DatabaseError("No database client set.")
        with self.db_client as db:
            try:
                update = ', '.join([f'{column} = :{column}' for column in columns])
                column_names = ', '.join(columns)
                placeholders = ', '.join([f":{column}" for column in columns])
                
                update_sql = f"UPDATE companies SET {update} WHERE business_id = :business_id"
                insert_sql = f"INSERT INTO companies ({column_names}) VALUES ({placeholders})"
                
                for data in batch_data:
                    params = {**dict(zip(columns, data)), "business_id": data[0]}
                    db._session.execute(text(update_sql), params)
                    
                    if db._session.execute("SELECT @@ROWCOUNT").scalar() == 0:
                        db._session.execute(text(insert_sql), params)
                
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error upserting company batch: {e}")

    def get_latest_bid(self):
        if self.db_client is None:
            raise DatabaseError("No database client set.")
        with self.db_client as db:
            try:
                result = db._session.execute(text(
                    "SELECT MAX(business_id) FROM companies WHERE business_id < '9000000-0'"
                )).fetchone()
                return result[0] if result else None
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error getting latest BID: {e}")

    def mark_empty_bid(self, bid):
        if self.db_client is None:
            raise DatabaseError("No database client set.")
        with self.db_client as db:
            try:
                insert_sql = "INSERT INTO unused_businessids (bid, checked) VALUES (:bid, :checked)"
                current = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db._session.execute(text(insert_sql), {"bid": bid, "checked": current})
            except SQLAlchemyError as e:
                raise DatabaseError(f"Error marking empty BID: {e}")

    def bid_checksum(self, bid):
        bid = str(bid).zfill(7)
        base = [int(x) for x in list(bid[:7])]
        mod = sum(np.multiply(base, [7, 9, 10, 5, 8, 4, 2])) % 11
        if mod == 1:
            return -1
        return 0 if mod == 0 else 11 - mod

    def check_bid(self, bid):
        bid = str(bid)
        bid_pattern = r'\d{7}-\d'
        if not re.match(bid_pattern, bid):
            return False
        check = int(bid[-1:])
        checksum = self.bid_checksum(bid)
        return checksum == check

    def generate_bids(self, start, count):
        start = int(str(start).partition('-')[0])
        bids = []
        while count:
            sbid = str(start).zfill(7)
            checksum = self.bid_checksum(sbid)
            start += 1
            if checksum > -1:
                fullbid = f"{sbid}-{checksum}"
                bids.append(fullbid)
                count -= 1
        return bids

    def load_bids_from_file(self, file_name):
        bids = []
        with open(file_name, 'r', encoding='utf-8') as file:
            for line in file:
                bid_pattern = r'(\d{7}-\d)'
                match = re.search(bid_pattern, line)
                if match:
                    bid = match.group(1)
                    if self.check_bid(bid):
                        bids.append(bid)
        return bids

    def load_bids_from_string(self, string):
        bids = []
        for line in string.split('\n'):
            bid_pattern = r'(\d{7}-\d)'
            match = re.search(bid_pattern, line)
            if match:
                bid = match.group(1)
                if self.check_bid(bid):
                    bids.append(bid)
        return bids

    def load_new_companies(self, next_bid, count):
        bids = self.generate_bids(next_bid, count)
        companies = self.get_multiple(bids)
        return companies

    def store_companies_to_db(self, companies, columns, progbar):
        if self.db_client is None:
            raise DatabaseError("No database client set.")
        with self.db_client as db:
            bartext = "Saving companies to the database..."
            progbar.progress(0, bartext)

            for i, company in enumerate(companies):
                company_data = self.parse_company(company)
                if company_data:
                    company_data.append(datetime.today().strftime('%Y-%m-%d'))
                    self.upsert_company(columns, company_data)
                progbar.progress((i / len(companies)), f"{bartext} ({i} of {len(companies)})")

            progbar.progress(100, bartext)
