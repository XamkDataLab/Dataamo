import pandas as pd
import numpy as np
import warnings

def read_eura2021_excel_to_dataframe():
    file_path = "eura2021_data.xlsx"

    # Naming convention: in English and in lower_snake_case
    column_mapping = {
        'Hankekoodi ': ('code', 'string'),
        'Ryhmähanketunnus': ('group_code', 'string'),
        'Rahasto': ('fund', 'string'),
        'EAKR-/JTF-alatyyppi': ('eakr_jtf_subtype', 'string'),
        'Tl': ('tl', 'string'),  # exception to the rule
        'Et': ('et', 'string'),  # exception to the rule
        'Hankkeen nimi': ('project_name', 'string'),
        'Toteuttajan nimi': ('implementing_organization', 'string'),
        'Toteuttajan Y-tunnus': ('implementing_organization_bid', 'string'),
        'Rahoittava viranomainen': ('funding_authority', 'string'),
        'Toiminnan tila': ('status', 'string'),
        'Aloituspvm': ('start_date', 'datetime64[ns]'),
        'Päättymispvm': ('end_date', 'datetime64[ns]'),
        'Suunniteltu EU- ja valtion rahoitus': ('planned_eu_and_state_funding', 'float64'),
        'Suunniteltu julkinen rahoitus': ('planned_public_funding', 'float64'),
        'Suunniteltu rahoitus yhteensä': ('planned_total_funding', 'float64'),
        'Toteutunut EU- ja valtion rahoitus': ('actual_eu_and_state_funding', 'float64'),
        'Toteutunut julkinen rahoitus': ('actual_public_funding', 'float64'),
        'Toteutunut rahoitus yhteensä': ('actual_total_funding', 'float64'),
        'Toteutunut EU-rahoituksen määrä': ('actual_eu_funding_amount', 'float64'),
        'Tukitoimen ala': ('funding_field', 'string'),
        'Sijainti': ('region', 'string'),
        'Toteutuspaikan postinumero': ('implementation_location_zipcode', 'string'),
        'Toteutuspaikan postitoimipaikka': ('implementation_location_region', 'string'),
        'Tuensaajan postinumero': ('beneficiary_zipcode', 'string'),
        'Tuensaajan postitoimipaikka': ('beneficiary_region', 'string'),
        'Tukimuoto': ('support_form', 'string'),
        'Alueellinen täytäntöönpanomekanismi ja alueellinen painopiste': ('regional_implementation_mechanism_and_priority', 'string'),
        'Taloudellinen toiminta': ('economic_activity', 'string'),
        'Toissijainen teema, vain ESR+': ('secondary_theme_esr_only', 'string'),
        'Sukupuolten tasa-arvo': ('gender_equality', 'string'),
        'Itämeren alueen strategia': ('baltic_sea_region_strategy', 'string'),
        'Suunnitelman mukainen tiivistelmä hankkeen toteutuksesta': ('project_implementation_summary_according_to_plan', 'string'),
        'Hankekuvauksen osoite': ('project_description_address', 'string'),
        'Hankkeen kotisivun osoite': ('project_website_address', 'string')
    }

    columns_in_english = {key: value[0] for key, value in column_mapping.items()}
    dtype_mapping = {key: value[1] for key, value in column_mapping.items()}

    # The generated Excel from EURA 2021 report has a non-standard style in it, works fine
    # but this skips the warning about it. Two last rows are just a footer (10.5.2024 anyway)
    #
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_excel(file_path, engine="openpyxl", skipfooter=2, dtype=dtype_mapping)

    df.rename(columns=columns_in_english, inplace=True)

    return df

if __name__ == "__main__":
    df = read_eura2021_excel_to_dataframe()

    print(df.head())