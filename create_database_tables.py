import pandas as pd
import re

def normalize_API_data(file):
    patentdata = pd.read_json(file)['data']
    df = pd.DataFrame(patentdata.values.tolist())
    df = pd.json_normalize(patentdata)
    return df
    
def list_length(list_value):
    return len(list_value)
    
def get_english_text(texts):
    texts = [text for text in texts if text['lang'] == 'en']
    if texts:
        return texts[0]['text']
    else:
        return texts[0]['text'] if texts else None
 
def clean_company_name(df, column):
    df[column] = df[column].str.lower()
    df[column] = df[column].str.replace(r'oy$', '', regex=True)
    df[column] = df[column].str.replace(r'ky$', '', regex=True)
    df[column] = df[column].str.replace(r'oyj$', '', regex=True)
    df[column] = df[column].str.replace(r'corp$', '', regex=True)
    df[column] = df[column].str.replace(r'[^a-z0-9åäö]', ' ', regex=True)
    return df
   
def applicants_table(file):
    df = normalize_API_data (file)
    applicants = df[['lens_id','doc_key','biblio.parties.applicants']].copy()
    applicants = applicants.explode('biblio.parties.applicants')
    applicants_normalized = pd.json_normalize(applicants['biblio.parties.applicants'])
    applicants_normalized = applicants_normalized.reset_index(drop=True)
    applicants = applicants.reset_index(drop=True)
    applicants = pd.concat([applicants[['lens_id', 'doc_key']], applicants_normalized], axis=1)
    applicants['nimi'] = applicants['extracted_name.value']
    return applicants
    
def inventors_table(file):
    df = normalize_API_data (file)
    inventors = df[['lens_id','doc_key','biblio.parties.inventors']].copy()
    inventors['biblio.parties.inventors'].fillna("")
    inventors = inventors.explode('biblio.parties.inventors')

    inventors_normalized = []
    for inventor in inventors['biblio.parties.inventors']:
        if type(inventor) == dict or (isinstance(inventor, list) and all(isinstance(i, dict) for i in inventor)):
            inventor_normalized = pd.json_normalize(inventor)
            inventors_normalized.append(inventor_normalized)
        else:
            inventor_normalized = pd.DataFrame({None: [None]})
            inventors_normalized.append(inventor_normalized)
    inventors_normalized = pd.concat(inventors_normalized, axis=0)
    inventors_normalized = inventors_normalized.reset_index(drop=True)
    inventors = inventors.reset_index(drop=True)
    inventors = pd.concat([inventors[['lens_id', 'doc_key','numInventors']], inventors_normalized], axis=1)
    return inventors
    
 def patents_table(file):
    df = normalize_API_data (file)
    df['numInventors'] = df['biblio.parties.inventors'].fillna("").apply(list_length)
    df['numApplicants'] = df['biblio.parties.applicants'].fillna("").apply(list_length)
    df['invention_title'] = df['biblio.invention_title'].apply(lambda x: get_english_text(x))
    df = df[['lens_id', 'jurisdiction', 'date_published', 'doc_key',
       'publication_type', 'biblio.publication_reference.jurisdiction',
       'biblio.publication_reference.doc_number',
       'biblio.publication_reference.kind',
       'biblio.publication_reference.date',
       'biblio.application_reference.jurisdiction',
       'biblio.application_reference.doc_number',
       'biblio.application_reference.kind',
       'biblio.application_reference.date',
       'biblio.priority_claims.earliest_claim.date',
       'invention_title','description.text','description.lang','numApplicants', 
       'numInventors','biblio.references_cited.patent_count',
       'biblio.references_cited.npl_count']].copy()
    return df
    
