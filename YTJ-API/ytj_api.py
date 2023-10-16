import os
import hashlib
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from zeep import Client, helpers
import pprint

load_dotenv()
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
    timestamp, token = get_timestamp_and_token()
    maxsize = 900 # How many items to get in one API call
    batches = np.array_split(bids, np.ceil(len(bids)/maxsize))

    for batch in batches:
        # Define the parameters for the operation
        params = {
            "ytunnus": ";".join(batch),
            "kieli": "fi",
            "asiakastunnus": customer_name,
            "aikaleima": timestamp,
            "tarkiste": token,
            "tiketti": ""
        }
        # Call the operation
        out += client.service.wmYritysTiedotMassahaku(**params)

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

def parse_company(company):
    
    data = helpers.serialize_object(company)

    if not data['YritysTunnus']['YTunnus']:
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

    if data['Yritysmuoto']:
        format = data['Yritysmuoto']['Seloste']

    if data['YritysTunnus']['Alkupvm']:
        # We are getting dates as "dd.mm.yyyy" but prefer "yyyy-mm-dd"
        d = data['YritysTunnus']['Alkupvm']
        registration = f"{d[6:]}-{d[3:5]}-{d[:2]}"

    print(businessid + ":", name, format, status, businessline, zipcode)

    return (businessid, name, format, businessline, zipcode, registration, status)

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
