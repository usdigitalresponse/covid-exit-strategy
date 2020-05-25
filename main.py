import datetime

import gspread
import pandas as pd
import requests
import scipy.interpolate as interpolate
from df2gspread import df2gspread
from oauth2client.service_account import ServiceAccountCredentials


# Define source field names.
DATE_SOURCE_FIELD = "date"
STATE_SOURCE_FIELD = "state"
TOTAL_CASES_SOURCE_FIELD = "positive"
LAST_UPDATED_SOURCE_FIELD = "dateModified"

# Define output field names.
LAST_UPDATED_FIELD = "last_updated"
TOTAL_CASES_3_DAY_AVERAGE_FIELD = "total_cases_3_day_average"
TOTAL_CASES_3_DAY_AVERAGE_CUBIC_SPLINE_FIELD = "total_cases_3_day_average_cubic_spline"
NEW_CASES_3_DAY_AVERAGE_FIELD = "new_cases_3_day_average"
NEW_CASES_3DCS_FIELD = "new_cases_3_day_average_cubic_spline"
NEW_CASES_FIELD = "new_cases"
NEW_CASES_DIFF_FIELD = "new_cases_compared_to_yesterday"
NEW_CASES_3DCS_DIFF_FIELD = "new_cases_compared_to_yesterday_3DCS"
CONSECUTIVE_INCREASE_NEW_CASES_3DCS_FIELD = "consecutive_increase_in_new_cases_3DCS"
CONSECUTIVE_DECREASE_NEW_CASES_3DCS_FIELD = "consecutive_decrease_in_new_cases_3DCS"
MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD = (
    "max_run_of_increasing_new_cases_in_14_day_window_3dcs"
)
MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD = (
    "max_run_of_decreasing_new_cases_in_14_day_window_3dcs"
)
CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD = (
    "cdc_criteria_1a_covid_continuous_decline"
)
CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD = "cdc_criteria_1b_covid_no_rebounds"
NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD = (
    "new_cases_compared_to_14_days_ago_3DCS"
)
CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD = "cdc_criteria_1c_covid_overall_decline"
CDC_CRITERIA_COMBINED_FIELD = "cdc_criteria_1_combined"

# Define the list of columns that should appear in the state summary tab.
STATE_SUMMARY_COLUMNS = [
    STATE_SOURCE_FIELD,
    DATE_SOURCE_FIELD,
    TOTAL_CASES_SOURCE_FIELD,
    TOTAL_CASES_3_DAY_AVERAGE_CUBIC_SPLINE_FIELD,
    NEW_CASES_3DCS_FIELD,
    MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD,
    CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD,
    MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD,
    CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD,
    NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD,
    CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD,
    CDC_CRITERIA_COMBINED_FIELD,
]

# Define the names of the tabs to upload to.
CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY = "1s534JoVjsetLDUxzkww3yQSnRj9H-8QLMKPUrq7RAuc"
FOR_WEBSITE_TAB_NAME = "For Website"
ALL_STATE_DATA_TAB_NAME = "All State Data"
WORK_IN_PROGRESS_CA_ONLY_TAB_NAME = "Work in Progress (California Only)"
STATE_SUMMARY_TAB_NAME = "State Summary"


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


def transform_covidtracking_data_old(df):
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

    # df.index = pd.MultiIndex(df[STATE_SOURCE_FIELD], df[DATE_SOURCE_FIELD])
    # df.set_index(keys=[STATE_SOURCE_FIELD, DATE_SOURCE_FIELD], inplace=True)

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
                TOTAL_CASES_SOURCE_FIELD,
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


def fit_and_predict_cubic_spline(series_):
    # Assert that the index is sorted.
    if not series_.index.is_monotonic_increasing:
        raise ValueError("Index is not sorted.")

    # Create a replacement index.
    # Note: the datetime index fails due to this line in the numpy code:
    # `if not np.all(diff(x) >= 0.0):`
    # The `diff(x)` generates timedeltas which cannot be compared to the float `0.0`.
    substitute_index = [i for i in range(len(series_))]

    # Note: you can't simply uses pd.Series.interpolate because that will only fill in data for the `nan` values.
    predicted_spline_values = interpolate.UnivariateSpline(
        x=substitute_index,
        y=series_.values,
        k=3,
        # For reference, the CDC uses this method with an `spar` of .5, as seen in line 384 of `Trajectory Analysis.R`:
        # https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/smooth.spline
        s=0.5,
    )(substitute_index)

    predicted_spline_series = pd.Series(
        data=predicted_spline_values, index=series_.index
    )

    return predicted_spline_series


def transform_covidtracking_data(df):
    states = df[STATE_SOURCE_FIELD].unique()

    # Make the date column explicitly a date
    df[DATE_SOURCE_FIELD] = pd.to_datetime(df[DATE_SOURCE_FIELD])

    # Use a multi-index for state and date.
    df.set_index(keys=[STATE_SOURCE_FIELD, DATE_SOURCE_FIELD], inplace=True)

    # Sort by the index: state ascending, date ascending.
    df = df.sort_index()

    for state in states:
        print(f"Processing state {state}...")
        # Calculate new cases (raw).
        df.loc[(state,), NEW_CASES_FIELD] = (
            df.loc[(state,), TOTAL_CASES_SOURCE_FIELD].diff(periods=1).values
        )

        # Calculate new cases (raw diff).
        df.loc[(state,), NEW_CASES_DIFF_FIELD] = (
            df.loc[(state,), NEW_CASES_FIELD].diff(periods=1).values
        )

        # Calculate 3-day rolling average of total cases.
        df.loc[(state,), TOTAL_CASES_3_DAY_AVERAGE_FIELD] = (
            df.loc[(state,), TOTAL_CASES_SOURCE_FIELD]
            .rolling(window=3, min_periods=1, center=False)
            .mean()
            .values
        )

        # Calculate the cubic spline on the 3 day average of total cases.
        df.loc[
            (state,), TOTAL_CASES_3_DAY_AVERAGE_CUBIC_SPLINE_FIELD
        ] = fit_and_predict_cubic_spline(
            df.loc[(state,), TOTAL_CASES_3_DAY_AVERAGE_FIELD]
        ).values

        # Calculate 3-day rolling average of new cases.
        df.loc[(state,), NEW_CASES_3_DAY_AVERAGE_FIELD] = (
            df.loc[(state,), NEW_CASES_FIELD]
            .rolling(window=3, min_periods=1, center=False)
            .mean()
            # Replace NA new cases with `0` to fit the spline.
            .fillna(value=0)
            .values
        )

        # Calculate the cubic spline on the 3 day average of total cases.
        df.loc[(state,), NEW_CASES_3DCS_FIELD] = fit_and_predict_cubic_spline(
            df.loc[(state,), NEW_CASES_3_DAY_AVERAGE_FIELD]
        ).values

        # Calculate 3DCS new cases diff.
        df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD] = (
            df.loc[(state,), NEW_CASES_3DCS_FIELD].diff(periods=1).values
        )

        # Calculate consecutive increases or decreases.
        df.loc[
            (state,), CONSECUTIVE_INCREASE_NEW_CASES_3DCS_FIELD
        ] = find_consecutive_positive_or_negative_values(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD], positive_values=True
        ).values

        df.loc[
            (state,), CONSECUTIVE_DECREASE_NEW_CASES_3DCS_FIELD
        ] = find_consecutive_positive_or_negative_values(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD], positive_values=False
        ).values

        # Calculate criteria 1A: must see at least 9 days of a decrease in new cases over a 14 day window.
        df.loc[
            (state,), MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
        ] = (
            df.loc[(state,), CONSECUTIVE_DECREASE_NEW_CASES_3DCS_FIELD]
            .rolling(window=14, min_periods=1, center=False)
            .max()
            .values
        )
        df.loc[(state,), CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD] = (
            df.loc[
                (state,), MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
            ]
            >= 10
        ).values

        # Calculate criteria 1B: must not see 5 or more days of an increase in new cases over a 14 day window.
        df.loc[
            (state,), MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
        ] = (
            df.loc[(state,), CONSECUTIVE_INCREASE_NEW_CASES_3DCS_FIELD]
            .rolling(window=14, min_periods=1, center=False)
            .max()
            .values
        )
        df.loc[(state,), CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD] = (
            df.loc[
                (state,), MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
            ]
            <= 5
        ).values

        # Calculate criteria 1C: new cases on T-0 must be < T-14.
        df.loc[(state,), NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD] = (
            df.loc[(state,), NEW_CASES_3DCS_FIELD].diff(periods=14).values
        )
        df.loc[(state,), CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD] = (
            df.loc[(state,), NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD] < 0
        ).values

        # Calculate all of the criteria combined in category 1.
        df.loc[(state,), CDC_CRITERIA_COMBINED_FIELD] = (
            df.loc[(state,), CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD]
            & df.loc[(state,), CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD]
            & df.loc[(state,), CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD]
        ).values

    # Remove the multi-index, converting date and state back to just columns.
    df = df.reset_index(drop=False)

    return df


def find_consecutive_positive_or_negative_values(series_, positive_values=True):
    meets_criteria = series_ >= 0 if positive_values else series_ < 0
    consecutive_positive_values = meets_criteria * (
        meets_criteria.groupby(
            (meets_criteria != meets_criteria.shift()).cumsum()
        ).cumcount()
        + 1
    )
    return consecutive_positive_values


def post_covidtracking_data(df, workbook_key, tab_name, credentials):
    print(f"Beginning to upload data to workbook {workbook_key} and tab {tab_name}...")
    df2gspread.upload(
        df=df,
        gfile=workbook_key,
        wks_name=tab_name,
        credentials=credentials,
        # Do not include the index in the upload.
        row_names=False,
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


def calculate_state_summary(transformed_df):
    # Find current date, and drop all other rows.
    current_date = transformed_df.loc[:, DATE_SOURCE_FIELD].max()

    state_summary_df = transformed_df.copy()
    state_summary_df = state_summary_df.loc[
        state_summary_df[DATE_SOURCE_FIELD] == current_date, STATE_SUMMARY_COLUMNS
    ]

    return state_summary_df


if __name__ == "__main__":
    df = load_covidtracking_historical_data()
    transformed_df = transform_covidtracking_data(df=df)

    client, credentials = get_sheets_client()

    state_summary = calculate_state_summary(transformed_df=transformed_df)

    # Upload data for just CA.
    post_covidtracking_data(
        df=transformed_df.loc[transformed_df[STATE_SOURCE_FIELD] == "CA",],
        workbook_key=CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY,
        tab_name=WORK_IN_PROGRESS_CA_ONLY_TAB_NAME,
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
