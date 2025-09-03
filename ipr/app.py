import os
import sys
import json
import streamlit as st
from streamlit_pills import pills

# This allows us to import modules from the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# db_api and ytj_api are folders in the parent directory
from db_api.database import DatabaseClient
from ytj_api.ytj import YtjClient

_ENV = "live"  # "live" or "local", changes the database connection

# Initialize session state
if 'raw_data' not in st.session_state:
    st.session_state.raw_data = ""

def fetch_data(progbar, get_records, get_bids, get_bids_fromfile, start_from=None):
    with DatabaseClient(env=_ENV) as db_client:
        ytj_client = YtjClient()
        ytj_client.set_database(db_client)

        if get_bids_fromfile is not None:
            bids = ytj_client.load_bids_from_string(get_bids_fromfile.read().decode('utf-8'))
        elif get_bids is not None:
            bids = ytj_client.load_bids_from_string(get_bids)
        else:
            if not start_from:
                latest_bid = ytj_client.get_latest_bid()
                start_from = int(str(latest_bid[:7])) + 1
            bids = ytj_client.generate_bids(start_from, get_records)

        companies = ytj_client.get_multiple(bids, progbar)

        columns = ['business_id', 'company', 'company_form', 'main_industry', 'postal_code', 'company_registration_date', 'status', 'hq', 'checked']

        ytj_client.store_companies_to_db(companies, columns, progbar)

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

    st.subheader(f"Database Info ({_ENV})")  # Add a section header

    with DatabaseClient(env=_ENV) as db_client:
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

start_from = None

# Show the corresponding form field based on the selected option
if selected_option == options[0]:
    get_records = st.number_input("Enter the number of records to fetch: (1-20000)", min_value=1, max_value=20000, value=100)
    start_from = st.text_input("Start from Business ID (optional):", "")
elif selected_option == options[1]:
    get_bids = st.text_area("Enter a list of business ids to fetch, each on a new line:", "")
elif selected_option == options[2]:
    get_bids_fromfile = st.file_uploader('Choose a file', type=['txt'])

if st.button("Fetch Company Data"):
    my_bar = st.progress(0, "Please wait")
    fetch_data(my_bar, get_records, get_bids, get_bids_fromfile, start_from)
    my_bar.progress(100, "Done!")
