import pandas
import re
from rapidfuzz import process, fuzz


def clean_company_name(name):
    stopwords = ['oy', 'ab', 'ky', 'oyj', 'gmbh', 'ltd', 'corp', 'inc', 'oy ab', 'rf', 'seura']
    name = name.lower()
    name = re.sub(r'[^a-z0-9åäöü]', ' ', name)
    name = re.sub(r'[ ]{2,}', ' ', name)
    # poistaa vain kun merkkijonon lopussa
    name = re.sub(r'(\b' + r'\b|\b'.join(stopwords) + r'\b)$', ' ', name)
    return name.strip()

import pyodbc

server = ''
database = ''
username = ''
password = ''
driver = ''

with pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT extracted_name FROM applicants")
        data = cursor.fetchall()

applicants = [clean_company_name(rows[0]) for rows in data]

with pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT yritys FROM yritykset")
        data = cursor.fetchall()

companies = [clean_company_name(rows[0]) for rows in data]

print(len(companies))

company_basenames = [clean_company_name(company) for company in companies]


def match_company_applicants(companies, applicants, cutoff):
    #    companies = df1.loc[:, col1].tolist()
    #    applicants = df2.loc[:, col2].unique().tolist()
    checked = 0
    total = len(companies)
    match_dict = {}
    for company in companies:
        matches = process.extract(company, applicants, scorer=fuzz.ratio, limit=10, score_cutoff=cutoff)
        for match in matches:
            match_dict[company] = []
            print(company, ":", match[0], "=", match[1])
            match_dict[company].append((match[0], match[1]))

        checked += 1
        if checked % 1000 == 0:
            percentage_completed = checked / total * 100
            print(f"{checked} companies have been checked. {percentage_completed:.2f}% completed.")

    #    df1["matches"] = df1[col1].map(match_dict)
    return match_dict


matches = match_company_applicants(company_basenames[200000:210000], applicants, 90)

