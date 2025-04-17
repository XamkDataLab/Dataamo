import os
import re
import hashlib
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers
from dateutil import parser

# Load the environment variables from the .env file
load_dotenv("../.env")
CUSTOMER_NAME = os.getenv("CUSTOMER_NAME")
API_KEY = os.getenv("API_KEY")

class YtjClient:
    def __init__(self, wsdl_url = None):
        # If no custom WSDL URL is provided, use the one in the same dir as this script
        if wsdl_url is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            wsdl_url = os.path.join(script_dir, "https_api_tietopalvelu_ytj_fi_yritystiedot.wsdl")
        
        self.client = Client(wsdl_url)
        self.db_client = None  # Initially, no database client is set

    def set_database(self, db_client):
        """Set the database client."""
        self.db_client = db_client

    def _get_timestamp_and_token(self):
        """Fetch the timestamp and token required for the paid YTJ API access"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        input_string = f"{CUSTOMER_NAME}{API_KEY}{timestamp}"
        sha1_token = hashlib.sha1(input_string.encode()).hexdigest()
        return timestamp, sha1_token

    def get_multiple(self, bids, progbar):
        out = []

        # YTJ API specification mentions that YTJ requests would return max 200 items, so
        # we keep the batch size under 195 (the API seems to return larger sets too, but to be safe)
        # The batch size is always in the range of 1-195
        #
        maxsize = max(1, min(len(bids) // 100, 195))
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

    @staticmethod
    def format_date(date_str):
        """
        Convert a date string (with or without time) to 'yyyy-mm-dd' format.
        If the input is None or invalid, return None.
        """
        if not date_str:
            return None

        # Define possible date formats
        date_formats = [
            "%d.%m.%Y %H:%M:%S",  # Format with time (e.g., "23.4.2019 11:52:05")
            "%d.%m.%Y",           # Format without time (e.g., "08.11.2000")
        ]

        for fmt in date_formats:
            try:
                # Try parsing the date string using the current format
                parsed_date = datetime.strptime(date_str, fmt)
                # Return the date in 'yyyy-mm-dd' format
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue  # If parsing fails, try the next format

        # If no format works, return None (or raise an error if needed)
        return None

    def extract_business_id_events(self, data):
        """
        Extracts business ID events (YritystunnusHistoria) from the data.

        Args:
            data (dict): The data object containing company information.

        Returns:
            list: A list of tuples containing (YTunnusVanha, YTunnusUusi, MuutosPvm, Tapahtuma).
        """
        events = []

        # Check if YritystunnusHistoria exists in the data
        if 'YritystunnusHistoria' in data and isinstance(data['YritystunnusHistoria'], dict):
            # Get the list of events (YritysTunnusHistoriaDTO)
            event_list = data['YritystunnusHistoria'].get('YritysTunnusHistoriaDTO', [])

            # Iterate through each event
            for event in event_list:
                if isinstance(event, dict):
                    # Extract the relevant fields
                    bid_old = event.get('YTunnusVanha')
                    bid_new = event.get('YTunnusUusi')
                    event_date = self.format_date(event.get('Muutospvm'))  # Format the date
                    event_desc = event.get('Tapahtuma')

                    # Append the tuple to the events list
                    events.append((bid_old, bid_new, event_date, event_desc))

        return events
    
    def extract_names(self, data, key):
        """Extract names and dates from the given data under the specified key."""
        names = []
        if key in data and isinstance(data[key], dict):
            name_list = data[key].get('ToiminimiDTO', [])
            if isinstance(name_list, list):  # Ensure it's a list before looping
                for item in name_list:
                    if isinstance(item, dict):
                        name = item.get('Toiminimi')
                        start_date = YtjClient.format_date(item.get('AlkuPvm'))
                        end_date = YtjClient.format_date(item.get('LoppuPvm'))

                        # Append the tuple to the names list
                        names.append((name, start_date, end_date))
        return names

    def get_registration_date(self, data):
        """
        Retrieves and formats the registration date from the data.
        First checks YritysTunnus, then falls back to YrityksenRekisteriHistoria.

        Args:
            data (dict): The data object containing company information.

        Returns:
            str or None: The formatted registration date, or None if not found.
        """
        # First, try to get the registration date from YritysTunnus
        registration_date = data.get('YritysTunnus', {}).get('Alkupvm')

        # If not found, check YrityksenRekisteriHistoria
        if not registration_date:
            yrityksen_rekisteri_historia = data.get('YrityksenRekisteriHistoria', {})
            yrityksen_rekisteri = yrityksen_rekisteri_historia.get('YrityksenRekisteri', []) if yrityksen_rekisteri_historia else []

            # Iterate through the list to find the first entry with Rekisterikoodi == "1"
            for entry in yrityksen_rekisteri:
                if entry.get('Rekisterikoodi') == '1':
                    registration_date = entry.get('Alkupvm')
                    break  # Stop after finding the first valid entry

        # Format the date using YtjClient.format_date
        if registration_date:
            return YtjClient.format_date(registration_date)
        else:
            return None  # Return None if no registration date is found
                
    def parse_company(self, company, verbose=False):
        data = helpers.serialize_object(company)

        if data is None or data['YritysTunnus'] is None or data['YritysTunnus']['YTunnus'] is None:
            return None

        businessid = data['YritysTunnus']['YTunnus']
        name = data['Toiminimi']['Toiminimi'] if data['Toiminimi'] else '[tyhjä]'
        if name == '[tyhjä]' and data.get('YrityksenHenkilo'):
            name = data['YrityksenHenkilo']['Nimi']

        status = data["YritysTunnus"].get("YrityksenLopettamisenSyy") or "Aktiivinen"
        businessline = (f"{data['Toimiala']['Seloste']} ({data['Toimiala']['Koodi']})" 
                        if data.get('Toimiala') else None)
        zipcode = (data['YrityksenPostiOsoite']['Postinumero'] 
                   if data.get('YrityksenPostiOsoite') else None)
        if not zipcode and data.get('YrityksenKayntiOsoite'):
            zipcode = data['YrityksenKayntiOsoite']['Postinumero']

        format = data.get("Yritysmuoto", {}).get("Seloste") or "[Ei tiedossa]"

        # Extracting and formatting the registration date
        registration = self.get_registration_date(data)

        # Extracting "Aputoiminimet" as a list of tuples (trade_name, start_date, end_date)
        trade_names = self.extract_names(data, 'Aputoiminimet')

        # Extracting "Rinnakkaistoiminimet" as a list of tuples (secondary_name, start_date, end_date)
        secondary_names = self.extract_names(data, 'Rinnakkaistoiminimet')

        # Extracting business ID events (YritystunnusHistoria)
        business_id_events = self.extract_business_id_events(data)

        if verbose:
            print(f"{businessid}: {name}, {format}, {status}, {businessline}, {zipcode}")

        return [businessid, name, format, businessline, zipcode, registration, status,
                trade_names,
                secondary_names,
                business_id_events]

    def upsert_company(self, columns, data):
        if self.db_client is None:
            raise RuntimeError("No database client set.")

        with self.db_client as db:
            try:
                # Ensure business_id is the first item in `data` to maintain consistency
                if 'business_id' not in columns:
                    raise ValueError("`columns` must include 'business_id'.")

                # Create a dictionary for the upsert method
                params = dict(zip(columns, data))
    
                # Call the generic upsert method from DatabaseClient
                db.upsert('companies', ['business_id'], params)

            except (RuntimeError, ValueError) as e:
                raise RuntimeError(f"Error upserting company: {e}")

    def upsert_company_batch(self, columns, batch_data):
        if self.db_client is None:
            raise RuntimeError("No database client set.")

        with self.db_client as db:
            try:
                if 'business_id' not in columns:
                    raise ValueError("`columns` must include 'business_id'.")

                for data in batch_data:
                    if len(data) != len(columns):
                        raise ValueError("Length of data does not match the number of columns.")
                    
                    # Create a dictionary for the upsert method
                    params = dict(zip(columns, data))
                    
                    # Call the generic upsert method from DatabaseClient
                    db.upsert('companies', 'business_id', params)

            except (RuntimeError, ValueError) as e:
                raise RuntimeError(f"Error upserting company batch: {e}")


    def get_latest_bid(self):
        if self.db_client is None:
            raise RuntimeError("No database client set.")
        with self.db_client as db:
            try:
                query = "SELECT MAX(business_id) FROM companies WHERE business_id < '9000000-0'"
                result = db.query(query)
                latest_bid = result[0][0] if result[0] else None
                return latest_bid
            except RuntimeError as e:
                raise RuntimeError(f"Error getting latest BID: {e}")

    def mark_empty_bid(self, bid):
        if self.db_client is None:
            raise RuntimeError("No database client set.")
        with self.db_client as db:
            try:
                insert_sql = "INSERT INTO unused_businessids (bid, checked) VALUES (:bid, :checked)"
                current = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db._session.execute(text(insert_sql), {"bid": bid, "checked": current})
            except RuntimeError as e:
                raise RuntimeError(f"Error marking empty BID: {e}")

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
            raise RuntimeError("No database client set.")
        with self.db_client as db:
            bartext = "Saving companies to the database..."
            progbar.progress(0, bartext)

            for i, company in enumerate(companies):
                company_data = self.parse_company(company)
                if company_data:
                    core_data = company_data[:7]
                    core_data.append(datetime.today().strftime('%Y-%m-%d'))
                    self.upsert_company(columns, core_data)

                    business_id = core_data[0]

                    trade_names = company_data[7]
                    if len(trade_names) > 0:
                        # Delete existing trade names for this business_id
                        db.delete("trade_names", "business_id", business_id)
                        
                        # Insert new trade names
                        for trade_name in trade_names:
                            trade_name_data = {
                                "business_id": business_id,
                                "trade_name": trade_name[0],
                                "start_date": trade_name[1],
                                "end_date": trade_name[2]
                            }
                            db.insert("trade_names", trade_name_data)

                    secondary_names = company_data[8]
                    if len(secondary_names) > 0:
                        # Delete existing trade names for this business_id
                        db.delete("secondary_names", "business_id", business_id)
                        
                        # Insert new trade names
                        for secondary_name in secondary_names:
                            secondary_name_data = {
                                "business_id": business_id,
                                "secondary_name": secondary_name[0],
                                "start_date": secondary_name[1],
                                "end_date": secondary_name[2]
                            }
                            db.insert("secondary_names", secondary_name_data)

                    business_id_events = company_data[9]
                    if len(business_id_events) > 0:
                        # Delete existing trade names for this business_id
                        db.delete("business_id_events", "business_id_new", business_id)
                        
                        # Insert new trade names
                        for business_id_event in business_id_events:
                            business_id_event_data = {
                                "business_id_old": business_id_event[0],
                                "business_id_new": business_id_event[1],
                                "event_date": business_id_event[2],
                                "event_desc": business_id_event[3]
                            }
                            db.insert("business_id_events", business_id_event_data)

                progbar.progress((i / len(companies)), f"{bartext} ({i} of {len(companies)})")

            progbar.progress(100, bartext)
