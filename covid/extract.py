import json

import pandas as pd
import requests
import covid.extract_config.analysis_usgovcloudapi as ugc


DATE_SOURCE_FIELD = "date"
STATE_SOURCE_FIELD = "state"
TOTAL_CASES_SOURCE_FIELD = "positive"
NEW_CASES_NEGATIVE_SOURCE_FIELD = "negativeIncrease"
NEW_CASES_POSITIVE_SOURCE_FIELD = "positiveIncrease"
LAST_UPDATED_SOURCE_FIELD = "dateModified"


def extract_covidtracking_current_data():
    current_url = "https://covidtracking.com/api/v1/states/current.json"
    current_data = requests.get(current_url).json()
    current_df = pd.DataFrame(current_data)

    return current_df


def extract_covidtracking_historical_data():
    historical_url = "https://covidtracking.com/api/v1/states/daily.json"
    historical_data = requests.get(historical_url).json()
    historical_df = pd.DataFrame(historical_data)

    historical_df[DATE_SOURCE_FIELD] = historical_df[DATE_SOURCE_FIELD].astype(str)

    return historical_df


def extract_state_population_data():
    # Note that the working directory is assumed to be the repository root.
    df = pd.read_csv("./covid/data/population.csv")

    df = df.set_index(keys=[STATE_SOURCE_FIELD])

    return df


def get_state_abbreviations_to_names():
    with open("./covid/data/us_state_abbreviations.json") as state_abbreviations_file:
        abbreviations = json.load(state_abbreviations_file)

    return abbreviations


def power_bi_extractor(response):
    data = json.loads(response.text)
    value_list = data['results'][0]['result']['data']['dsr']['DS'][0]['PH'][1]['DM1']
    for vl in value_list:
        yield vl['C']


# CDC data source: https://www.cdc.gov/nhsn/covid19/report-patient-impact.html

def extract_cdc_inpatient_beds():
    # State Representative Estimates for Percentage of Inpatient Beds Occupied (All Patients)
    response = requests.post(
        ugc.URL,
        headers={**ugc.BASE_HEADERS, **ugc.INPATIENT_BED_HEADERS},
        data=open("./covid/extract_config/inpatient_bed_query.json"))

    df = pd.DataFrame(
        power_bi_extractor(response),
        columns=["State", "inpatient_bed_percent_occupied", "inpatient_beds_occupied"])

    df = df.set_index('State')

    return df


def extract_cdc_icu_beds():
    # State Representative Estimates for Percentage of ICU Beds Occupied (All Patients)
    response = requests.post(
        ugc.URL,
        headers={**ugc.BASE_HEADERS, **ugc.ICU_BED_HEADERS},
        data=open("./covid/extract_config/icu_bed_query.json"))

    df = pd.DataFrame(
        power_bi_extractor(response),
        columns=["State", "icu_percent_occupied", "icu_beds_occupied"])

    df = df.set_index('State')

    return df


def extract_cdc_facilities_reporting():
    response = requests.post(
        ugc.URL,
        headers={**ugc.BASE_HEADERS, **ugc.FACILITIES_REPORTING_HEADERS},
        data=open("./covid/extract_config/facilities_reporting_query.json"))
    
    df = pd.DataFrame(
        power_bi_extractor(response),
        columns=["State", "facilities_percent_reporting", "facilities_reporting"])

    df = df.set_index('State')

    return df
