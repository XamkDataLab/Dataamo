import re
from zeep import Client, helpers
from collections import OrderedDict
from icecream import ic
from datetime import datetime
import progressbar

import db_api as db

import ytj_api as ytj
#import importlib
#importlib.reload(ytj)

_ENV = "live" # "live" or "local", changes the database connection

print("Initializing connection...")
connection = db.init_connection(_ENV)
if connection == None:
    exit()

connection.autocommit = True
cursor = connection.cursor()

print("Loading the business ids...")
bids = []
file_name = "bids.txt"

with open(file_name, 'r', encoding='utf-8') as file:
    for line in file:
        # Some table exports have extra characters at the end and at the beginning of the line
        bid_pattern = r'(\d{7}-\d)'
        match = re.search(bid_pattern, line)
        if match:
            bid = match.group(1)
            if ytj.check_bid(bid):
                bids.append(bid)

print("Reading company information from YTJ...")
companies = ytj.get_multiple(bids)

columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status', 'tarkastettu']

print("Storing the company data to the database...")

# Create a progress bar widget
progress = progressbar.ProgressBar(max_value=len(bids))
progress.update(0)

for i, company in enumerate(companies):
#    print(company)
    company_data = ytj.parse_company(company)
    if(company_data):
        company_data.append(datetime.today().strftime('%Y-%m-%d'))
        ytj.upsert_company(connection, columns, company_data)
    progress.update(i)

# Finish the progress bar
progress.finish()

print("Last business id processed was", bids[-1])

cursor.close()
connection.close()