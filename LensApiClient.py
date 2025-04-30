import requests
import json
import time
from pprint import pprint

import pandas as pd

import sys
from pathlib import Path

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

token = "wteI1cLIVBIep5TA4zDxtPEgMNnnXLWydIufgIkl3k16jkOYCQAk"

# Get the absolute path to the db-api directory
current_file_path = Path(__file__).absolute()
project_root = current_file_path.parent
db_api_path = project_root / "db-api"  # /Dataamo/db-api

# Add to Python's module search path
sys.path.insert(0, str(db_api_path))

try:
    from database import DatabaseClient
except ImportError as e:
    print("Import failed:", e)
    exit()

def get_patent_data(start_date, end_date, token):
    url = 'https://api.lens.org/patent/search'
    token = token
    include = '["lens_id","date_published","jurisdiction","biblio","doc_key","description","publication_type"]'
    request_body = f'''{{
        "query": {{
            "bool": {{
                "must": [ 
                    {{"term": {{"applicant.residence": "FI"}}}},
                    {{"range": {{"date_published": {{"gte": "{start_date}", "lte": "{end_date}"}}}}}}
                ]
            }}
        }},
        "include": {include},
        "scroll": "1m",
        "size": 100
    }}'''
    headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}

    patents = []
    scroll_id = None

    while True:

        if scroll_id is not None:
            request_body = f'{{"scroll_id": "{scroll_id}","include": {include}}}'
        response = requests.post(url, data=request_body, headers=headers)
        if response.status_code == requests.codes.too_many_requests:
            print("TOO MANY REQUESTS, waiting...")
            time.sleep(8)
            continue
        if response.status_code != requests.codes.ok:
            print("ERROR:", response)
            break

        response = response.json()
        
        # Safely get data (empty list if missing)
        patents.extend(response.get('data', []))
        
        # Print progress (use 0 if 'total' is missing)
        print(f"{len(patents)} / {response.get('total', 0)} patenttia luettu...")
        
        # Safely get scroll_id (None if missing)
        scroll_id = response.get('scroll_id')
        
        # Check if we should break (also handles missing 'total')
        if (response.get('total', 0) <= len(patents)) or not response.get('data'):
            break

    data_out = {"total": len(patents), "data": patents}

    return json.dumps(data_out)

def get_patent_title(titles):
    """Extract title with preference for English, fallback to first available"""
    if not titles:
        return "No Title Available"
    
    # Try to find English title first
    en_title = next((t for t in titles if t.get("lang") == "en"), None)
    if en_title:
        return en_title.get("text", "")
    
    # Fallback to first available title with language marker
    first_title = titles[0]
    lang = first_title.get("lang", "unknown")
    return f"{first_title.get('text', '')} ({lang})"

def save_pprint_to_file(data, filename):
  """
  Prints a dictionary using pprint and saves the output to a text file.
  """
  with open(filename, 'w', encoding='utf-8') as f:
    pprint(data, stream=f)

def upsert_records(df, table_name, key_columns):
    """
    Generic upsert method for all entity types
    
    Args:
        df: DataFrame containing records to upsert
        table_name: Name of the database table
        key_columns: List of column names that form the unique key
    """
    with DatabaseClient(env="local") as db_client:
        try:
            for _, row in df.iterrows():
                # Create data dictionary dynamically from row
                data = {col: row[col] for col in df.columns if col != 'id'}
                db_client.upsert(table_name, key_columns, data)

            print(f"Upserted {len(df)} records to {table_name} table")

        except Exception as e:
            print(f"Failed to upsert to {table_name}: {str(e)}")
            raise

def process_month(year, month):
    # Calculate start and end dates for the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    print(f"\nProcessing month: {start_date.strftime('%Y-%m')}")
    
    patents = get_patent_data(start_date_str, end_date_str, token)
    patents_dict = json.loads(patents)

    if not patents_dict['data']:
        print(f"No patent data for {start_date.strftime('%Y-%m')} - {end_date.strftime('%Y-%m')}")
        return

    patents_list = []
    inventors_list = []
    applicants_list = []

    for idx, patent in enumerate(patents_dict["data"]):
        lens_id = patent.get("lens_id", "")
        publication_type = patent.get("publication_type", "")
        date_published = patent.get("date_published", "")
        jurisdiction = patent.get("jurisdiction", "")
        biblio = patent.get("biblio", {})
        titles = biblio.get("invention_title", [])
        parties = biblio.get("parties", {})
        inventors = parties.get("inventors", [])
        applicants = parties.get("applicants", [])

        patents_list.append({
            "id": idx + 1,
            "lens_id": lens_id,
            "publication_type": publication_type,
            "date_published": date_published,
            "invention_title": get_patent_title(titles),
            "jurisdiction": jurisdiction
        })
        
        for inventor in inventors:
            inventor_name = str(inventor.get("extracted_name", {}).get("value", "")).title()
            inventor_address = inventor.get("extracted_address", "")
            inventor_orcid = inventor.get("orcid", "")
            inventor_residence = inventor.get("residence", "")
            
            inventors_list.append({
                "id": idx + 1,
                "lens_id": lens_id,
                "name": inventor_name,
                "address": inventor_address,
                "orcid": inventor_orcid,
                "residence": inventor_residence
            })

        for applicant in applicants:
            applicant_name = applicant.get("extracted_name", {}).get("value", "").title()
            applicant_address = applicant.get("extracted_address", "")
            
            applicants_list.append({
                "id": idx + 1,
                "lens_id": lens_id,
                "extracted_name": applicant_name,
                "extracted_address": applicant_address
            })

    patents_df = pd.DataFrame(patents_list)
    applicants_df = pd.DataFrame(applicants_list)
    inventors_df = pd.DataFrame(inventors_list)

    # Filter out non-FI inventors before upsert
    original_count = len(inventors_df)
    inventors_df = inventors_df[(inventors_df['residence'].isna()) | (inventors_df['residence'] == "FI")]
    filtered_count = len(inventors_df)
    
    print(f"Filtered out {original_count - filtered_count} non-FI inventor records")

    # Then in process_month(), replace the individual upsert calls with:
    upsert_records(patents_df, "patents", ["lens_id"])
    upsert_records(applicants_df, "applicants", ["lens_id", "extracted_name"])
    upsert_records(inventors_df, "inventors", ["lens_id", "name"])

# Start from a more current date and go backwards
current_date = datetime(1989, 12, 1)
end_date = datetime(1980, 1, 1)  # Stop when we reach this date

while current_date >= end_date:
    process_month(current_date.year, current_date.month)
    # Move to previous month
    current_date = current_date - relativedelta(months=1)
