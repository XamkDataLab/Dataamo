import re
from datetime import datetime
import progressbar

from database import DatabaseClient
from ytj import YtjClient

_ENV = "local" # "live" or "local", changes the database connection

def main():
    print("Initializing connection...")
    db_client = DatabaseClient(_ENV)
    if db_client.connection is None:
        exit()
    db_client.connection.autocommit = True

    ytj_client = YtjClient()

    print("Loading the business ids...")
    bids = ytj_client.load_bids_from_file("bids.txt")

    print("Reading company information from YTJ...")
    companies = ytj_client.get_multiple(bids)

    columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status', 'tarkastettu']

    print("Storing the company data to the database...")
    ytj_client.store_companies_to_db(companies, columns, db_client.connection)

    print("Last business id processed was", bids[-1])

    db_client.connection.close()

if __name__ == "__main__":
    main()