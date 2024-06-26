import os
import re
import hashlib
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers
import progressbar
from datetime import datetime

load_dotenv(".env")
customer_name = os.getenv("CUSTOMER_NAME")

# Initialize the SOAP client with the YTJ WSDL file
client = Client('https_api_tietopalvelu_ytj_fi_yritystiedot.wsdl')

# Generates a current timestamp and calculates a valid YTJ API token ("tunniste", this is not the API key)
#
def get_timestamp_and_token():
    # Read the customer specific values from .env
    api_key = os.getenv("API_KEY")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Combine the inputs into a single string
    input_string = f"{customer_name}{api_key}{timestamp}"

    # Calculate the SHA-1 hash
    sha1_token = hashlib.sha1(input_string.encode()).hexdigest()

    return timestamp, sha1_token

def get_multiple(bids):
    out = []
    maxsize = max(5, len(bids)//100) # Maximum of many items to get in one API call
    batches = np.array_split(bids, np.ceil(len(bids)/maxsize))

    # Create a progress bar widget
    progress = progressbar.ProgressBar(max_value=len(bids))
    progress.update(0)

    for i, batch in enumerate(batches):
        # timestamp might get old so we calculate it for each batch
        timestamp, token = get_timestamp_and_token()
        # Define the parameters for the operation
        params = {
            "ytunnus": ";".join(batch),
            "kieli": "fi",
            "asiakastunnus": customer_name,
            "aikaleima": timestamp,
            "tarkiste": token,
            "tiketti": ""
        }
        # Call the API
        out += client.service.wmYritysTiedotMassahaku(**params)
        
        # Update the progress bar
        progress.update(i * maxsize)

    # Finish the progress bar
    progress.finish()

    return out

def search(str = "", bid = ""):
    timestamp, token = get_timestamp_and_token()

    params = {
        "hakusana": str,
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

    # Call the operation
    response = client.service.wmYritysHaku(**params)

    return response

def parse_company(company, verbose = False):
    
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

def upsert_company(connection, columns, data):
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

# In the late 1970's a few business ids on the range from 9000000 upwards where given out,
# so the latest business id is the max id below that range.
#
def get_latest_bid(connection):
    cursor = connection.cursor()
    sql = "SELECT MAX(y_tunnus) FROM yritykset WHERE y_tunnus < '9000000-0'"
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0]

def mark_empty_bid(connection, bid):
    cursor = connection.cursor()
    insert_sql = f'INSERT INTO unused_businessids ("bid", "checked") VALUES (?, ?)'
    current = datetime.now()
    cursor.execute(insert_sql, bid, current)    

# Because "0 == False" equals True, we use "-1" as "invalid"
#
def bid_checksum(bid):
    bid = str(bid).zfill(7)
    base = [int(x) for x in list(bid[:7])]
    mod = sum(np.multiply(base, [7,9,10,5,8,4,2])) % 11
    if mod == 1:
        return -1
    return 0 if (mod == 0) else (11 - mod)

def check_bid(bid):
    bid = str(bid)
    bid_pattern = r'\d{7}-\d'
    if not re.match(bid_pattern, bid):
        return False
    check = int(bid[-1:])
    checksum = bid_checksum(bid)
    return checksum == check

def generate_bids(start, count):
    # If an existing business id is given, fetch the number portion of it.
    start = int(str(start).partition('-')[0])
    bids = []
    while(count):
        sbid = str(start).zfill(7)
        checksum = bid_checksum(sbid)
        start += 1
        # Every 11th business id is skipped, don't count the skipped ones
        if checksum > -1:
            fullbid = str(sbid) + "-" + str(checksum)
            bids.append(fullbid)
            count -= 1
    return bids
