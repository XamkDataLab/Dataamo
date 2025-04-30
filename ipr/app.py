import sys
from pathlib import Path

_ENV = "local"  # "live" or "local", changes the database connection

# Get the absolute path to the db-api directory
current_file_path = Path(__file__).absolute()  # /Dataamo/ipr/app.py
project_root = current_file_path.parent.parent  # /Dataamo
db_api_path = project_root / "db-api"  # /Dataamo/db-api
ytj_api_path = project_root / "ytj-api" / "ytj"  # /Dataamo/ytj-api/ytj

# Add to Python's module search path
sys.path.insert(0, str(db_api_path))
sys.path.insert(0, str(ytj_api_path))

try:
    from database import DatabaseClient
    from ytj import YtjClient
except ImportError as e:
    print("Import failed:", e)
    exit()

import streamlit as st
from streamlit_pills import pills

def fetch_data(progbar, get_records, get_bids, get_bids_fromfile):
    # _ENV = "local"  # "live" or "local", changes the database connection

    with DatabaseClient(env = _ENV) as db_client:
        ytj_client = YtjClient()
        ytj_client.set_database(db_client)

        if get_bids_fromfile is not None:
            bids = ytj_client.load_bids_from_string(get_bids_fromfile.read().decode('utf-8'))
        elif get_bids is not None:
            bids = ytj_client.load_bids_from_string(get_bids)
        else:
            latest_bid = ytj_client.get_latest_bid()
            next_bid = int(str(latest_bid[:7])) + 1
            bids = ytj_client.generate_bids(next_bid, get_records)

        companies = ytj_client.get_multiple(bids, progbar)
        columns = ['business_id', 'company', 'company_form', 'main_industry', 'postal_code', 'company_registration_date', 'status', 'checked']
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
