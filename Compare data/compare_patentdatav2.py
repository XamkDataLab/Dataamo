import pandas as pd
import re
from rapidfuzz import process, fuzz

list_of_lut_possibilities = ["lappeenrannan lahden teknillinen yliopisto lut",
                             "lappeenrannan lahden teknillinen yliopisto",
                             "lappeenrannan lahden teknillinen yliopisto lut",
                             "lappeenrannan teknillinen ylio", "lappeenrannan laehden teknillinen yliopisto lut",
                             "lappeenranta lahti univ of technology lut", "lappeenranta univ of tech",
                             "lappeenranta univ of technology",
                             "lappeenranta university of technology"]

list_of_abo_possibilities = ["åbo akadem", "abo akademi abo akademi university", "åbo akademi åbo akademi university",
                             "abo akademi abo akademi univ", "åbo akademi åbo akademi univ", "abo akademi univ",
                             "åbo akademi univ", "abo akademi university", "åbo akademi university"]


def change_name(oname, list, cname):
    if oname in list:
        return cname
    else:
        return oname


def clean_company_name(name, use_changename):
    stopwords_end = ['oy', 'ab', 'ky', 'oyj', 'gmbh', 'ltd', 'corp', 'inc', 'oy ab', 'rf', 'seura', 'osuuskunta',
                     'ab oy']
    pattern_for_end_words = r'(\b' + r'\b|\b'.join(stopwords_end) + r'\b)$'
    stopwords_start = ['oy', 'ab', 'oy ab', 'osakeyhtiö', 'osakeyhtioe', 'osuuskunta', 'ab oy']
    pattern_for_start_words = r'^(' + '|'.join(stopwords_start) + r')\b\s*'

    name = name.lower()
    name = re.sub(r'[^a-z0-9åäöü]', ' ', name)
    name = re.sub(r'[ ]{2,}', ' ', name)

    # poistaa vain kun merkkijonon lopussa
    name = re.sub(pattern_for_end_words, '', name)

    # poistaa vain kun merkkijonojen alussa
    name = re.sub(pattern_for_start_words, '', name)

    # tarkistetaan käytetäänko nimen muokkausta, näin ollen nimen muokkausta ei ole pakko käyttää molempien datojen
    # muotoilussa
    if use_changename:
        name = change_name(name, list_of_lut_possibilities, "lappeenrannan teknillinen yliopisto")
        name = change_name(name, list_of_abo_possibilities, "åbo akademi")

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

applicants_data = [clean_company_name(rows[0], True) for rows in data]

with pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT yritys FROM yritykset")
        data = cursor.fetchall()

companies_data = [clean_company_name(rows[0], False) for rows in data]

print(len(companies_data))

company_basenames = [clean_company_name(company, False) for company in companies_data]


def match_company_applicants(companies, applicants, score):
    checked = 0
    matched_rows = []
    total = len(companies)
    for company in companies:
        matches = process.extract(company, applicants, scorer=fuzz.ratio, limit=10, score_cutoff=score)
        for match in matches:
            space_index = match[0].find(" ")
            applicant_string_parts = match[0].split(" ", 1)
            company_string_parts = company.split(" ", 1)
            if space_index != -1 and len(company_string_parts) > 1:
                first_score = fuzz.ratio(company_string_parts[0], applicant_string_parts[0])
                second_score = fuzz.ratio(company_string_parts[1], applicant_string_parts[1])
                if (len(applicant_string_parts[0]) <= 3 or len(
                        company_string_parts[0]) <= 3) and first_score == 100 and second_score >= score:
                    matched_rows.append({'Company': company, 'Applicant': match[0], 'Score': match[1]})
                elif (3 < len(applicant_string_parts[0]) <= 6 or 3 < len(
                        company_string_parts[0]) <= 6) and first_score >= score and second_score >= score:
                    matched_rows.append({'Company': company, 'Applicant': match[0], 'Score': match[1]})
                elif (len(applicant_string_parts[0]) > 6 or len(
                        company_string_parts[0]) > 6) and first_score >= score and second_score >= score:
                    matched_rows.append({'Company': company, 'Applicant': match[0], 'Score': match[1]})
                else:
                    continue
            elif len(match[0]) <= 3 and match[1] == 100:
                matched_rows.append({'Company': company, 'Applicant': match[0], 'Score': match[1]})
            else:
                matched_rows.append({'Company': company, 'Applicant': match[0], 'Score': match[1]})

        checked += 1
        if checked % 1000 == 0:
            percentage_completed = checked / total * 100
            print(f"{checked} companies have been checked. {percentage_completed:.2f}% completed.")

    matched_df = pd.DataFrame(matched_rows)
    return matched_df


compared_data = match_company_applicants(company_basenames[200000:210000], applicants_data, 90)

with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3,
                       ):
    print(compared_data)
