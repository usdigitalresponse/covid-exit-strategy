import datetime

import pandas as pd
import requests
from df2gspread import df2gspread
import gspread
from oauth2client.service_account import ServiceAccountCredentials

DATE_SOURCE_FIELD = "date"
STATE_SOURCE_FIELD = "state"
POSITIVE_CASES_SOURCE_FIELD = "positive"
LAST_UPDATED_SOURCE_FIELD = "dateModified"
LAST_UPDATED_FIELD = "last_updated"
CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD = "cdc_criteria_1c_covid_overall_decline"

CDC_GUIDANCE_GOOGLE_SHEET_KEY = "1s534JoVjsetLDUxzkww3yQSnRj9H-8QLMKPUrq7RAuc"


def load_covidtracking_current_data():
    current_url = "https://covidtracking.com/api/v1/states/current.json"
    current_data = requests.get(current_url).json()
    current_df = pd.DataFrame(current_data)

    return current_df


def load_covidtracking_historical_data():
    historical_url = "https://covidtracking.com/api/v1/states/daily.json"
    historical_data = requests.get(historical_url).json()
    historical_df = pd.DataFrame(historical_data)

    historical_df[DATE_SOURCE_FIELD] = historical_df[DATE_SOURCE_FIELD].astype(str)

    return historical_df


def transform_covidtracking_data(df):
    total_case_column_name_formatter = "total_cases_t_minus_{}"
    new_case_column_name_formatter = "new_cases_t_minus_{}"

    # TODO(lbrown): Refactor this to be more efficient; this is just the quick and dirty way.
    states = df[STATE_SOURCE_FIELD].unique()

    number_of_lags = 121

    column_names = [LAST_UPDATED_FIELD, CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD]

    for column_formatter in [
        total_case_column_name_formatter,
        new_case_column_name_formatter,
    ]:
        column_names.extend(
            [column_formatter.format(lag) for lag in range(number_of_lags)]
        )

    transformed_df = pd.DataFrame(index=states, columns=column_names)

    for state in states:
        # Start each state looking up today.
        date_to_lookup = datetime.datetime.now()

        for lag in range(number_of_lags):
            print(f"Processing {state} for lag {lag}.")
            # Lookup the historical entry:
            value = df.loc[
                (df[STATE_SOURCE_FIELD] == state)
                & (df[DATE_SOURCE_FIELD] == date_to_lookup.strftime("%Y%m%d")),
                POSITIVE_CASES_SOURCE_FIELD,
            ]

            if len(value) > 1:
                raise ValueError("Too many or too few values returned.")
            elif len(value) == 1:
                value = value.iloc[0]
                transformed_df.loc[
                    state, total_case_column_name_formatter.format(lag)
                ] = value

            date_to_lookup = date_to_lookup - datetime.timedelta(days=1)

    for state in states:
        for lag in range(number_of_lags - 1):
            # Calculate new cases.
            previous_value = transformed_df.loc[
                state, total_case_column_name_formatter.format(lag + 1)
            ]
            previous_value = previous_value if previous_value else 0

            value = transformed_df.loc[
                state, total_case_column_name_formatter.format(lag)
            ]

            transformed_df.loc[state, new_case_column_name_formatter.format(lag)] = (
                value - previous_value
            )

        for lag in range(number_of_lags - 1):
            # Calculate new cases.
            previous_value = transformed_df.loc[
                state, total_case_column_name_formatter.format(lag + 1)
            ]
            previous_value = previous_value if previous_value else 0

            value = transformed_df.loc[
                state, total_case_column_name_formatter.format(lag)
            ]

            transformed_df.loc[state, new_case_column_name_formatter.format(lag)] = (
                value - previous_value
            )

    # Now, run analysis for CDC gating criteria.
    # Criteria 2: COVID Cases.
    # Criteria 2c: 14th day must be lower than 1st day.
    transformed_df[CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD] = (
        transformed_df.loc[:, new_case_column_name_formatter.format(0)]
        <= transformed_df.loc[:, new_case_column_name_formatter.format(14)]
    )

    # Add an update time.
    transformed_df[LAST_UPDATED_FIELD] = datetime.datetime.now()

    return transformed_df


def post_covidtracking_data(df, credentials):
    print("Beginning to upload data...")
    df2gspread.upload(
        df=df,
        gfile=CDC_GUIDANCE_GOOGLE_SHEET_KEY,
        wks_name="ForWebsite",
        credentials=credentials,
        row_names=True,
        col_names=True,
    )
    print("Finished uploading data.")


# TODO(lbrown): this was created when I was using the Sheets API, at this point we may only need the credentials.
def get_sheets_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "service-account-key.json", scope
    )
    client = gspread.authorize(credentials)

    return client, credentials


if __name__ == "__main__":
    df = load_covidtracking_historical_data()
    df = transform_covidtracking_data(df=df)

    client, credentials = get_sheets_client()

    post_covidtracking_data(df=df, credentials=credentials)
