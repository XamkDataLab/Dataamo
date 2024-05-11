from zeep import Client, helpers
from collections import OrderedDict
import ytj_api as ytj
import db_api as db
from datetime import datetime
from icecream import ic
import progressbar

def main():

    _GET_RECORDS = 50
    _ENV = "live" # "live" or "local", changes the database connection

    # Define the batch size for processing
    _BATCH_SIZE = 50

    print("Initializing connection...")
    connection = db.init_connection(_ENV)
    if connection == None:
        exit()

    connection.autocommit = True
    cursor = connection.cursor()

    latest_bid = ytj.get_latest_bid(connection)
    next_bid = int(str(latest_bid[:7])) + 1

    print("Generating new business ids...")
    bids = ytj.generate_bids(next_bid, _GET_RECORDS)

    print("Reading new company information...")
    companies = ytj.get_multiple(bids)

    columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status', 'tarkastettu']

    print("Storing the new company data to the database...")
    empty = 0

    # Create a progress bar widget
    progress = progressbar.ProgressBar(max_value=len(bids))
    progress.update(0)

    # Initialize an empty batch list
    #batch_data = []

    for i, company in enumerate(companies):
        company_data = ytj.parse_company(company)
        if company_data:
            company_data.append(datetime.today().strftime('%Y-%m-%d'))
            ytj.upsert_company(connection, columns, company_data)
            empty = 0
        else:
            empty += 1    

        # Update the progress bar
        progress.update(i)

    # Finish the progress bar
    progress.finish()

    print("Last business id processed was", bids[-1])

    cursor.close()
    connection.close()
  
if __name__ == "__main__":
    main()