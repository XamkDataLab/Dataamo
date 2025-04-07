from zeep import Client, helpers
from collections import OrderedDict
import ytj_api as ytj
import db_api as db
from icecream import ic
import progressbar

_GET_RECORDS = 10000
_ENV = "live" # "live" or "local", changes the database connection

print("Initializing connection...")
connection = db.init_connection(_ENV)
connection.autocommit = True
cursor = connection.cursor()

latest_bid = ytj.get_latest_bid(connection)
next_bid = int(str(latest_bid[:7])) + 1

print("Generating new business ids...")
bids = ytj.generate_bids(next_bid, _GET_RECORDS)

print("Reading new company information...")
companies = ytj.get_multiple(bids)

columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status']

print("Storing the new company data to the database...")
empty = 0

# Create a progress bar widget
progress = progressbar.ProgressBar(max_value=len(bids))
progress.update(0)

for i, company in enumerate(companies):
    company_data = ytj.parse_company(company)
    if(company_data):
        ytj.upsert_company(connection, columns, company_data)
        empty = 0
    else:
# :TODO: Find out when the data stops, for example the date is recent and we get X amount of
#        concurrent empty entries...
#
#        ytj.mark_empty_bid(cursor, "abcde")
        empty += 1
#        print("Concurrent empty entries so far:", empty)
    # Update the progress bar
    progress.update(i)

# Finish the progress bar
progress.finish()

print("Last business id processed was", bids[-1])

cursor.close()
connection.close()