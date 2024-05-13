import sys
import argparse
from ytj import YtjClient
from database import DatabaseClient

def main():
    _GET_RECORDS = 50
    _ENV = "local"  # "live" or "local", changes the database connection

    db_client = DatabaseClient(_ENV)
    if db_client.connection is None:
        exit()

    db_client.connection.autocommit = True

    ytj_client = YtjClient()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load business IDs from file or generate new ones.')
    parser.add_argument('--file', help='File name for loading business IDs from')
    args = parser.parse_args()

    # Retrieve file name from arguments
    file_name = args.file

    if file_name:
        print("Loading business ids from file...")
        bids = ytj_client.load_bids_from_file(file_name)
    else:
        print("Loading new business ids...")
        latest_bid = ytj_client.get_latest_bid(db_client.connection)
        next_bid = int(str(latest_bid[:7])) + 1
        bids = ytj_client.generate_bids(next_bid, _GET_RECORDS)

    print("Reading company information from YTJ...")
    companies = ytj_client.get_multiple(bids)

    columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status', 'tarkastettu']

    print("Storing the company data to the database...")
    ytj_client.store_companies_to_db(companies, columns, db_client.connection)

    print("Last business id processed was", bids[-1])

    db_client.connection.close()

if __name__ == "__main__":
    main()
