from ytj import YtjClient
from database import DatabaseClient
from datetime import datetime
import progressbar

def main():
    _GET_RECORDS = 5000
    _ENV = "local" # "live" or "local", changes the database connection

    # Define the batch size for processing
    _BATCH_SIZE = 50

    print("Initializing connection...")
    db_client = DatabaseClient(_ENV)
    if db_client.connection is None:
        exit()

    db_client.connection.autocommit = True
    ytj_client = YtjClient()

    latest_bid = ytj_client.get_latest_bid(db_client.connection)
    next_bid = int(str(latest_bid[:7])) + 1

    print("Loading new company information")
    companies = ytj_client.load_new_companies(next_bid, _GET_RECORDS)

    columns = ['y_tunnus', 'yritys', 'yhtiömuoto', 'toimiala', 'postinumero', 'yrityksen_rekisteröimispäivä', 'status', 'tarkastettu']

    print("Storing the new company data to the database...")
    ytj_client.store_companies_to_db(companies, columns, db_client.connection)

    print("Last business id processed was", bids[-1])

#    cursor.close()
    db_client.connection.close()
  
if __name__ == "__main__":
    main()