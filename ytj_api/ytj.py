import os
import re
import hashlib
import numpy as np
import pprint
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers
from sqlalchemy.sql import text
from datetime import datetime

# Constants for data indices
BUSINESS_ID = 0
TRADE_NAMES = 8
SECONDARY_NAMES = 9
PREVIOUS_NAMES = 10
EVENTS = 11

# Load the environment variables from the .env file
load_dotenv(".env")
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

    def _fetch_previous_names(self, bid):
        """Fetch previous names for a given business ID using wmToiminimi."""
        timestamp, token = self._get_timestamp_and_token()
        params = {
            "ytunnus": bid,
            "asiakastunnus": CUSTOMER_NAME,
            "aikaleima": timestamp,
            "tarkiste": token,
            "tiketti": "",
            "kieli": "fi"
        }

        # Call the service
        response = self.client.service.wmToiminimi(**params)
        serialized_response = helpers.serialize_object(response)

        # If no EdellinenTieto → nothing to do
        edellinen_tieto = serialized_response.get("EdellinenTieto")
        if not edellinen_tieto:
            return []

        # If no YTieto → nothing to do
        previous_names_list = edellinen_tieto.get("YTieto")
        if not previous_names_list:
            return []

        # The API response can sometimes be a single dictionary, not a list
        if isinstance(previous_names_list, dict):
            previous_names_list = [previous_names_list]

        names = []
        for item in previous_names_list:
            if not item:  # skip empty/null items
                continue
            name = item.get("Tieto")
            start_date = self.format_date(item.get("Alkupvm"))
            end_date = self.format_date(item.get("Loppupvm"))
            if name:
                names.append((name, start_date, end_date))

        return names


    def debug_pretty_print_company(self, company):
        # Serialize the company object
        data = helpers.serialize_object(company)

        # Pretty print the serialized data
        pp = pprint.PrettyPrinter(indent=2, width=120, compact=False)
        pp.pprint(data)

        return 

    def get_multiple(self, bids, progbar):
        out = []

        maxsize = max(20, min(len(bids) // 100, 195))
        maxsize = min(maxsize, len(bids))
        
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

        print(out)

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
        Extracts business ID events from the company data.
        """
        # Use .get() to safely access 'YritystunnusHistoria'
        yritystunnus_historia = data.get('YritystunnusHistoria')

        # Check if yritystunnus_historia is None before trying to use .get() on it
        if yritystunnus_historia is None:
            return []

        # Now it is safe to use .get() on yritystunnus_historia, as we know it's not None
        event_list = yritystunnus_historia.get('YritysTunnusHistoriaDTO', [])
        
        # Ensure event_list is always a list for consistent iteration
        if not isinstance(event_list, list):
            event_list = [event_list]  # Wrap single objects in a list
        
        events = []

        # Safely get the 'YritystunnusHistoria' dictionary or an empty dictionary if it's not present.
        yritystunnus_historia = data.get('YritystunnusHistoria', {})

        # Get the list of events (YritysTunnusHistoriaDTO) or an empty list if not found.
        event_list = yritystunnus_historia.get('YritysTunnusHistoriaDTO', [])
        
        # Ensure event_list is always a list for consistent iteration.
        if not isinstance(event_list, list):
            event_list = [event_list]

        # Iterate through each event.
        for event in event_list:
            if isinstance(event, dict):
                # Extract the relevant fields.
                bid_old = event.get('YTunnusVanha')
                bid_new = event.get('YTunnusUusi')
                event_date = self.format_date(event.get('Muutospvm'))
                event_desc = event.get('Tapahtuma')

                # Append the tuple to the events list.
                events.append((bid_old, bid_new, event_date, event_desc))

        return events
    
    def extract_names(self, data, key):
        """Extract names and dates from the given data under the specified key."""
        names = []

        # Safely get the nested dictionary or an empty one
        name_container = data.get(key, {})
        if not isinstance(name_container, dict):
            return names

        # Safely get the list of names or an empty list
        name_list = name_container.get('ToiminimiDTO', [])
        if not isinstance(name_list, list):
            # If it's a single dictionary, wrap it in a list for consistent iteration
            name_list = [name_list] if name_list else []
        
        for item in name_list:
            if not isinstance(item, dict):
                continue

            name = item.get('Toiminimi')
            start_date = YtjClient.format_date(item.get('AlkuPvm'))
            end_date = YtjClient.format_date(item.get('LoppuPvm'))
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

            # Ensure yrityksen_rekisteri is a list
            if not isinstance(yrityksen_rekisteri, list):
                yrityksen_rekisteri = [yrityksen_rekisteri]

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

        # If data is not a dictionary, or does not have the required key, return None.
        # This prevents the AttributeError from occurring.
        if not isinstance(data, dict) or not data.get('YritysTunnus'):
            print("ERROR: Serialized data is not a dictionary. Returning None.")
            return None

        # This line can also cause an error if 'YritysTunnus' exists but is not a dict.
        # We can handle this with a safe get, which is already in the original code.
        ytunnus_info = data.get('YritysTunnus', {})
        if not ytunnus_info.get('YTunnus'):
            return None

        businessid = ytunnus_info['YTunnus']

        toiminimi_data = data.get('Toiminimi')
        if toiminimi_data:
            name = toiminimi_data.get('Toiminimi')
        else:
            name = None

        # Fallback to default name if no name is found
        if not name:
            name = '[tyhjä]'
            if data.get('YrityksenHenkilo'):
                name = data['YrityksenHenkilo'].get('Nimi')

        #status = data.get("YritysTunnus", {}).get("YrityksenLopettamisenSyy") or "Aktiivinen"

        status = data.get("YritysTunnus", {}).get("YrityksenLopettamisenSyy")

        # Check if ElinkeinoToiminta indicates business has ended
        elinkeino = data.get("ElinkeinoToiminta", {})
        if elinkeino.get("Seloste") == "Elinkeinotoiminta päättynyt":
            status = "Toiminta lakannut"

        # Fallback if no status was set
        if not status:
            status = "Aktiivinen"

        businessline = (f"{data['Toimiala']['Seloste']} ({data['Toimiala']['Koodi']})" 
                        if data.get('Toimiala') and isinstance(data['Toimiala'], dict) else None)
        zipcode = (data.get('YrityksenPostiOsoite', {}).get('Postinumero') 
                if data.get('YrityksenPostiOsoite') else None)
        if not zipcode and data.get('YrityksenKayntiOsoite'):
            zipcode = data.get('YrityksenKayntiOsoite', {}).get('Postinumero')

        # Get the 'Kotipaikka' data and convert 'Seloste' to title case, store in "hq" column
        hq = (data.get('Kotipaikka', {}).get('Seloste') or '').title() or None
        
        format = data.get("Yritysmuoto", {}).get("Seloste") or "[Ei tiedossa]"

        registration = self.get_registration_date(data)
        trade_names = self.extract_names(data, 'Aputoiminimet')
        secondary_names = self.extract_names(data, 'Rinnakkaistoiminimet')

        previous_names = []
        if format in ['Osakeyhtiö', 'Julkinen osakeyhtiö'] and name != '[tyhjä]':
            previous_names = self._fetch_previous_names(businessid)

        business_id_events = self.extract_business_id_events(data)

        if verbose:
            print(f"{businessid}: {name}, {format}, {status}, {businessline}, {zipcode}, {hq}")

        return [businessid, name, format, businessline, zipcode, registration, status, hq,
                trade_names,
                secondary_names,
                previous_names,
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
                db.upsert('companies', 'business_id', params)

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
                if company_data is None:
                    print(f"Skipping empty or invalid company object: {company}")
                    continue

                self._store_core_data(db, columns, company_data)
                self._store_names_data(db, company_data, "trade_names", TRADE_NAMES, "trade_name")
                self._store_names_data(db, company_data, "secondary_names", SECONDARY_NAMES, "secondary_name")
                self._store_names_data(db, company_data, "previous_names", PREVIOUS_NAMES, "previous_name")
                self._store_business_id_events(db, company_data)

                progbar.progress((i / len(companies)), f"{bartext} ({i} of {len(companies)})")

            progbar.progress(100, bartext)

    def _store_core_data(self, db, columns, company_data):
        core_data = company_data[:8]
        core_data.append(datetime.today().strftime('%Y-%m-%d'))
        self.upsert_company(columns, core_data)

    def _store_names_data(self, db, company_data, table_name, data_index, name_field):
        business_id = company_data[BUSINESS_ID]
        names = company_data[data_index]
        if names:
            db.delete(table_name, "business_id", business_id)
            for name in names:
                db.insert(table_name, {
                    "business_id": business_id,
                    name_field: name[0],
                    "start_date": name[1],
                    "end_date": name[2]
                })

    def _store_business_id_events(self, db, company_data):
        business_id = company_data[BUSINESS_ID]
        events = company_data[EVENTS]
        if events:
            db.delete("business_id_events", "business_id_new", business_id)
            for event in events:
                db.insert("business_id_events", {
                    "business_id_old": event[0],
                    "business_id_new": event[1],
                    "event_date": event[2],
                    "event_desc": event[3]
                })
