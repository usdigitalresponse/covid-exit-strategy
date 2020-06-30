# Define source field names.
import os
import time

from covid.constants import PATH_TO_SERVICE_ACCOUNT_KEY
from covid.extract import extract_cdc_beds_current_data
from covid.extract import extract_cdc_beds_historical_data
from covid.extract import extract_cdc_ili_data
from covid.extract import extract_covidtracking_historical_data
from covid.load import get_sheets_client
from covid.load import post_dataframe_to_google_sheets
from covid.transform import CRITERIA_1_SUMMARY_COLUMNS
from covid.transform import CRITERIA_2_SUMMARY_COLUMNS
from covid.transform import CRITERIA_3_SUMMARY_COLUMNS
from covid.transform import CRITERIA_5_SUMMARY_COLUMNS
from covid.transform import CRITERIA_6_SUMMARY_COLUMNS
from covid.transform import STATE_SUMMARY_COLUMNS
from covid.transform import transform_cdc_beds_data
from covid.transform import transform_cdc_ili_data
from covid.transform import transform_covidtracking_data
from covid.transform_utils import calculate_state_summary

# Define the names of the tabs to upload to.
CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY = "1s534JoVjsetLDUxzkww3yQSnRj9H-8QLMKPUrq7RAuc"
FOR_WEBSITE_TAB_NAME = "For Website"
ALL_STATE_DATA_TAB_NAME = "All State Data"
WORK_IN_PROGRESS_NY_ONLY_TAB_NAME = f"{ALL_STATE_DATA_TAB_NAME} (NY Only)"
STATE_SUMMARY_TAB_NAME = "State Summary"

CDC_CRITERIA_1_GOOGLE_WORKBOOK_KEY = "1p4Z6zTa6O0ss5B5rgoWotAIwsBqqEqwdcM3Yel-mm5g"
CDC_CRITERIA_2_GOOGLE_WORKBOOK_KEY = "1xdePOZkXXv49_15YTloLr7D72eQhY9R-ZEhoMr-4UY0"
CDC_CRITERIA_3_GOOGLE_WORKBOOK_KEY = "1-BSd5eFbNsypygMkhuGX1OWoUsF2u4chpsu6aC4cgVo"
CDC_CRITERIA_5_GOOGLE_WORKBOOK_KEY = "1t9PbnAJBGKMBPPH-cwHxXca38ZVrkBrLhIBl6S66oFM"
CDC_CRITERIA_6_GOOGLE_WORKBOOK_KEY = "1xhKoRK5GZBqor3Cn16K89ZtZGN9Iq93ShnsXCIqctK0"

# Note: if you'd like to run the full pipeline, you'll need to generate a service account keyfile for an account
# that has been given write access to the Google Sheet.


def extract_transform_and_load_covid_data():
    print("Starting to ETL...")

    client, credentials = get_sheets_client(
        credential_file_path=os.path.abspath(PATH_TO_SERVICE_ACCOUNT_KEY)
    )

    cdc_beds_current_df = extract_cdc_beds_current_data()
    cdc_beds_historical_df = extract_cdc_beds_historical_data(credentials)
    covidtracking_df = extract_covidtracking_historical_data()
    cdc_ili_df = extract_cdc_ili_data()

    transformed_cdc_beds_df = transform_cdc_beds_data(
        cdc_beds_current_df=cdc_beds_current_df,
        cdc_beds_historical_df=cdc_beds_historical_df,
    )

    transformed_cdc_ili_df = transform_cdc_ili_data(ili_df=cdc_ili_df)

    transformed_covidtracking_df = transform_covidtracking_data(
        covidtracking_df=covidtracking_df
    )

    # Upload category 3A data.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_cdc_beds_df, columns=CRITERIA_3_SUMMARY_COLUMNS
        ),
        workbook_key=CDC_CRITERIA_3_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    post_dataframe_to_google_sheets(
        df=transformed_cdc_beds_df,
        workbook_key=CDC_CRITERIA_3_GOOGLE_WORKBOOK_KEY,
        tab_name="Historical Data",
        credentials=credentials,
    )

    time.sleep(20)

    # Upload summary for all states.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_covidtracking_df, columns=STATE_SUMMARY_COLUMNS
        ),
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    # Upload Criteria 1 workbook for all states.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_covidtracking_df,
            columns=CRITERIA_1_SUMMARY_COLUMNS,
        ),
        workbook_key=CDC_CRITERIA_1_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    # Upload Criteria 2 workbook for all states.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_covidtracking_df,
            columns=CRITERIA_2_SUMMARY_COLUMNS,
        ),
        workbook_key=CDC_CRITERIA_2_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    # Upload Criteria 5 workbook
    # Upload all data tab for Criteria 5.
    post_dataframe_to_google_sheets(
        df=transformed_cdc_ili_df,
        workbook_key=CDC_CRITERIA_5_GOOGLE_WORKBOOK_KEY,
        tab_name=ALL_STATE_DATA_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    # Upload state summary tab for Criteria 5.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_cdc_ili_df, columns=CRITERIA_5_SUMMARY_COLUMNS
        ),
        workbook_key=CDC_CRITERIA_5_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    time.sleep(20)

    # Upload state summary tab for Criteria 6.
    post_dataframe_to_google_sheets(
        df=calculate_state_summary(
            transformed_df=transformed_covidtracking_df,
            columns=CRITERIA_6_SUMMARY_COLUMNS,
        ),
        workbook_key=CDC_CRITERIA_6_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    # It appears we're uploading so much data we're hitting a quota of some kind. So pause before this last spreadsheet,
    # which is enormous.
    time.sleep(20)

    # Upload data for all states.
    post_dataframe_to_google_sheets(
        df=transformed_covidtracking_df,
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=ALL_STATE_DATA_TAB_NAME,
        credentials=credentials,
    )


if __name__ == "__main__":
    extract_transform_and_load_covid_data()
