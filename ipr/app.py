import streamlit as st
from streamlit_pills import pills

import time

from ytj import YtjClient
from database import DatabaseClient

def fetch_data(progbar, get_records, get_bids, get_bids_fromfile):
    _ENV = "live"  # "live" or "local", changes the database connection

    with DatabaseClient(env=_ENV) as db_client:
        if not db_client.connection:
            st.error("Database connection failed. Exiting.")
            return

        ytj_client = YtjClient()

        if get_bids_fromfile is not None:
            #print("Loading business ids from a text input...")
            bids = ytj_client.load_bids_from_string(get_bids_fromfile.read().decode('utf-8'))
            #print(bids)
            return False
        if get_bids is not None:
            #print("Loading business ids from a text input...")
            bids = ytj_client.load_bids_from_string(get_bids)
        else:
            #print("Generating new business ids...")
            latest_bid = ytj_client.get_latest_bid(db_client.connection)
            next_bid = int(str(latest_bid[:7])) + 1
            bids = ytj_client.generate_bids(next_bid, get_records)

        #print("Reading company information from YTJ...")
        companies = ytj_client.get_multiple(bids, progbar)

        columns = ['business_id', 'company', 'company_form', 'main_industry', 'postal_code', 'company_registration_date', 'status', 'checked']

        print("Storing the company data to the database...")
        ytj_client.store_companies_to_db(companies, columns, db_client.connection, progbar)

        print("Last business id processed was", bids[-1])

st.set_page_config(
    page_title="Data Fetcher",
    page_icon="📊",
    layout="centered",
)

with st.sidebar:
    st.markdown("""
        # Data Fetcher
        Reads company information from YTJ and writes it to the IPR Suomi database.
    """)

    st.subheader("Database Info")  # Add a section header    

    with DatabaseClient(env="live") as db_client:
        if not db_client.connection:
            st.error("Database connection failed.")
            st.stop()

        with st.spinner("Loading..."):
            sql = "SELECT indicator, value FROM ipr_suomi_dbinfo"
            info = db_client.query(sql)
            st.dataframe(info)

# Create a horizontal selector using streamlit-pills
options = ["#️⃣ Number of new records to fetch", "📃 List of business ids", "📁 File with a list of business ids"]
selected_option = pills("Select the input method:", options)

get_records = None
get_bids = None
get_bids_fromfile = None

# Show the corresponding form field based on the selected option
if selected_option == options[0]:
    get_records = st.number_input("Enter the number of records to fetch: (1-5000)", min_value=1, max_value=5000, value=100)
elif selected_option == options[1]:
    get_bids = st.text_area("Enter a list of business ids to fetch, each on a new line:", "")
elif selected_option == options[2]:
    get_bids_fromfile = st.file_uploader('Choose a file', type=['txt'])

if st.button("Fetch Company Data"):
    my_bar = st.progress(0, "Please wait")
    fetch_data(my_bar, get_records, get_bids, get_bids_fromfile)
    my_bar.progress(100, "Done!")
