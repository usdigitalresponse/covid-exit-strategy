# Define source field names.
import os

from covid.extract import extract_covidtracking_historical_data
from covid.extract import STATE_SOURCE_FIELD
from covid.load import get_sheets_client
from covid.load import post_covidtracking_data
from covid.transform import calculate_state_summary
from covid.transform import transform_covidtracking_data

# Define the names of the tabs to upload to.
CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY = "1s534JoVjsetLDUxzkww3yQSnRj9H-8QLMKPUrq7RAuc"
FOR_WEBSITE_TAB_NAME = "For Website"
ALL_STATE_DATA_TAB_NAME = "All State Data"
WORK_IN_PROGRESS_NY_ONLY_TAB_NAME = f"{ALL_STATE_DATA_TAB_NAME} (NY Only)"
STATE_SUMMARY_TAB_NAME = "State Summary"

# Note: if you'd like to run the full pipeline, you'll need to generate a service account keyfile for an account
# that has been given write access to the Google Sheet.
PATH_TO_SERVICE_ACCOUNT_KEY = "service-account-key.json"


def extract_transform_and_load_covid_data():
    df = extract_covidtracking_historical_data()
    transformed_df = transform_covidtracking_data(df=df)

    client, credentials = get_sheets_client(
        credential_file_path=os.path.abspath(PATH_TO_SERVICE_ACCOUNT_KEY)
    )

    state_summary = calculate_state_summary(transformed_df=transformed_df)

    # Upload data for just NY.
    post_covidtracking_data(
        df=transformed_df.loc[transformed_df[STATE_SOURCE_FIELD] == "NY",],
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=WORK_IN_PROGRESS_NY_ONLY_TAB_NAME,
        credentials=credentials,
    )

    # Upload summary for all states.
    post_covidtracking_data(
        df=state_summary,
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=STATE_SUMMARY_TAB_NAME,
        credentials=credentials,
    )

    # Upload data for all states.
    post_covidtracking_data(
        df=transformed_df,
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=ALL_STATE_DATA_TAB_NAME,
        credentials=credentials,
    )


if __name__ == "__main__":
    extract_transform_and_load_covid_data()
