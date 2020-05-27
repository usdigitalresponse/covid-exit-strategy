import datetime
import json

import gspread
import numpy as np
import pandas as pd
import requests
import rpy2.robjects as robjects
import scipy.interpolate as interpolate
from df2gspread import df2gspread
from oauth2client.service_account import ServiceAccountCredentials


# Define source field names.
DATE_SOURCE_FIELD = "date"
STATE_SOURCE_FIELD = "state"
TOTAL_CASES_SOURCE_FIELD = "positive"
NEW_CASES_NEGATIVE_SOURCE_FIELD = "negativeIncrease"
NEW_CASES_POSITIVE_SOURCE_FIELD = "positiveIncrease"
LAST_UPDATED_SOURCE_FIELD = "dateModified"

# Define output field names.
# Criteria Category 1 Fields.
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
TOTAL_NEW_CASES_IN_14_DAY_WINDOW_FIELD = "total_new_cases_in_14_day_window"
TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_FIELD = (
    "total_new_cases_in_14_day_window_per_100k_population"
)
TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_LOWER_THAN_THRESHOLD_FIELD = (
    "total_new_cases_in_14_day_window_per_100k_population_lower_than_threshold"
)
TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_PREVIOUSLY_ELEVATED_FIELD = "total_new_cases_in_14_day_window_per_100k_population_previously_higher_than_threshold"
CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD = (
    "cdc_criteria_1a_covid_continuous_decline"
)
CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD = "cdc_criteria_1b_covid_no_rebounds"
NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD = (
    "new_cases_compared_to_14_days_ago_3DCS"
)
CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD = "cdc_criteria_1c_covid_overall_decline"
CDC_CRITERIA_1D_COVID_NEAR_ZERO_INCIDENCE = "cdc_criteria_1d_covid_near_zero_incidence"
CDC_CRITERIA_1_COMBINED_FIELD = "cdc_criteria_1_combined"


# Criteria Category 2 Fields.
NEW_TESTS_TOTAL_FIELD = "new_tests_total"
NEW_TESTS_TOTAL_DIFF_FIELD = "new_tests_total_compared_to_yesterday"
PERCENT_POSITIVE_NEW_TESTS_FIELD = "percent_positive_new_tests"
PERCENT_POSITIVE_NEW_TESTS_DIFF_FIELD = (
    "percent_positive_new_tests_compared_to_yesterday"
)
MAX_RUN_OF_INCREASING_PERCENT_POSITIVE_TESTS_FIELD = (
    "max_run_of_increasing_percent_positive_tests"
)
MAX_RUN_OF_DECREASING_PERCENT_POSITIVE_TESTS_FIELD = (
    "max_run_of_decreasing_percent_positive_tests"
)
MAX_RUN_OF_INCREASING_TOTAL_TESTS_FIELD = "max_run_of_increasing_total_tests"

CDC_CRITERIA_2A_COVID_PERCENT_CONTINUOUS_DECLINE_FIELD = (
    "cdc_criteria_2a_covid_percent_continuous_decline"
)

CDC_CRITERIA_2B_COVID_TOTAL_TEST_VOLUME_INCREASING_FIELD = (
    "cdc_criteria_2b_covid_total_test_volume_increasing"
)

CDC_CRITERIA_2C_COVID_PERCENT_OVERALL_DECLINE_FIELD = (
    "cdc_criteria_2c_covid_percent_overall_decline"
)

CDC_CRITERIA_2D_COVID_NEAR_ZERO_POSITIVE_TESTS_FIELD = (
    "cdc_criteria_2c_covid_near_zero_positive_tests"
)
CDC_CRITERIA_2_COMBINED_FIELD = "cdc_criteria_2_combined"

# Other fields
CDC_CRITERIA_ALL_COMBINED_FIELD = "cdc_criteria_all_combined"
LAST_UPDATED_FIELD = "last_updated"

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
    CDC_CRITERIA_1D_COVID_NEAR_ZERO_INCIDENCE,
    CDC_CRITERIA_1_COMBINED_FIELD,
]

# Define the names of the tabs to upload to.
CDC_GUIDANCE_GOOGLE_WORKBOOK_KEY = "1s534JoVjsetLDUxzkww3yQSnRj9H-8QLMKPUrq7RAuc"
FOR_WEBSITE_TAB_NAME = "For Website"
ALL_STATE_DATA_TAB_NAME = "All State Data"
WORK_IN_PROGRESS_NY_ONLY_TAB_NAME = "Work in Progress (NY Only)"
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
        x=substitute_index, y=series_.values, k=3
    )(substitute_index)

    predicted_spline_series = pd.Series(
        data=predicted_spline_values, index=series_.index
    )

    return predicted_spline_series


def fit_and_predict_cubic_spline_in_r(
    series_, smoothing_parameter=None, replace_nan=True
):
    if not smoothing_parameter:
        # Import `NULL` from R.
        smoothing_parameter = robjects.r["as.null"]()

    # Assert that the index is sorted.
    if not series_.index.is_monotonic_increasing:
        raise ValueError("Index is not sorted.")

    # Replace nans
    if replace_nan:
        series_ = series_.fillna(value=0)

    r_x = robjects.DateVector(series_.index)
    r_y = robjects.FloatVector(series_.values.astype(float))

    # Extract R's smoothing function.
    r_smooth_spline = robjects.r["smooth.spline"]

    # For reference, the CDC uses this method with an `spar` of .5, as seen in line 384 of `Trajectory Analysis.R`:
    # https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/smooth.spline
    fitted_spline = r_smooth_spline(x=r_x, y=r_y, spar=smoothing_parameter)

    predicted_spline_values = list(
        robjects.r["predict"](fitted_spline, robjects.FloatVector(r_x)).rx2("y")
    )

    predicted_spline_series = pd.Series(
        data=predicted_spline_values, index=series_.index
    )

    return predicted_spline_series


def load_state_population_data():
    df = pd.read_csv("./data/population.csv")

    with open("./data/us_state_abbreviations.json") as state_abbreviations_file:
        abbreviations = json.load(state_abbreviations_file)

    df = df.replace({STATE_SOURCE_FIELD: abbreviations})

    df = df.set_index(keys=[STATE_SOURCE_FIELD])

    return df


def transform_covidtracking_data(df):
    states = df[STATE_SOURCE_FIELD].unique()

    # Make the date column explicitly a date
    df[DATE_SOURCE_FIELD] = pd.to_datetime(df[DATE_SOURCE_FIELD])

    # Use a multi-index for state and date.
    df.set_index(keys=[STATE_SOURCE_FIELD, DATE_SOURCE_FIELD], inplace=True)

    # Sort by the index: state ascending, date ascending.
    df = df.sort_index()

    # Load state population data.
    state_population_data = load_state_population_data()

    for state in states:
        print(f"Processing state {state}...")

        ###### Calculate criteria category 1. ######
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
        ] = fit_and_predict_cubic_spline_in_r(
            series_=df.loc[(state,), TOTAL_CASES_3_DAY_AVERAGE_FIELD],
            smoothing_parameter=0.5,
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
        df.loc[(state,), NEW_CASES_3DCS_FIELD] = fit_and_predict_cubic_spline_in_r(
            series_=df.loc[(state,), NEW_CASES_3_DAY_AVERAGE_FIELD],
            smoothing_parameter=0.5,
        ).values

        # Calculate 3DCS new cases diff.
        df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD] = (
            df.loc[(state,), NEW_CASES_3DCS_FIELD].diff(periods=1).values
        )

        # Calculate consecutive increases or decreases.
        df.loc[
            (state,), CONSECUTIVE_INCREASE_NEW_CASES_3DCS_FIELD
        ] = get_consecutive_positive_or_negative_values(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD], positive_values=True
        ).values

        df.loc[
            (state,), CONSECUTIVE_DECREASE_NEW_CASES_3DCS_FIELD
        ] = get_consecutive_positive_or_negative_values(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD], positive_values=False
        ).values

        # Calculate criteria 1A: must see at least 9 days of a decrease in new cases over a 14 day window.
        df.loc[
            (state,), MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
        ] = get_max_run_in_window(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD],
            positive_values=False,
            window_size=14,
        ).values

        df.loc[(state,), CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD] = (
            df.loc[
                (state,), MAX_RUN_OF_DECREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
            ]
            >= 10
        ).values

        # Calculate criteria 1B: must not see 5 or more days of an increase in new cases over a 14 day window.
        df.loc[
            (state,), MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
        ] = get_max_run_in_window(
            series_=df.loc[(state,), NEW_CASES_3DCS_DIFF_FIELD],
            positive_values=True,
            window_size=14,
        ).values

        df.loc[(state,), CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD] = (
            df.loc[
                (state,), MAX_RUN_OF_INCREASING_NEW_CASES_IN_14_DAY_WINDOW_3DCS_FIELD
            ]
            < 5
        ).values

        # Calculate criteria 1C: new cases on T-0 must be < T-14.
        df.loc[(state,), NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD] = (
            df.loc[(state,), NEW_CASES_3DCS_FIELD].diff(periods=14).values
        )
        df.loc[(state,), CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD] = (
            df.loc[(state,), NEW_CASES_TODAY_MINUS_NEW_CASES_14_DAYS_AGO_3DCS_FIELD] < 0
        ).values

        # Calculate criteria 1D: total cases from the last 14 days must be less than 10 per 100k population.
        df.loc[(state,), TOTAL_NEW_CASES_IN_14_DAY_WINDOW_FIELD] = (
            df.loc[(state,), NEW_CASES_FIELD]
            .fillna(value=0)
            .rolling(window=14, min_periods=1, center=False)
            .sum()
            .values
        )

        state_population = float(state_population_data.loc[state][0])
        df.loc[
            (state,), TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_FIELD
        ] = (
            (100000.0 * df.loc[(state,), TOTAL_NEW_CASES_IN_14_DAY_WINDOW_FIELD])
            / state_population
        ).values

        df.loc[
            (state,),
            TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_LOWER_THAN_THRESHOLD_FIELD,
        ] = (
            df.loc[
                (state,), TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_FIELD
            ]
            <= 10
        ).values

        df.loc[
            (state,),
            TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_PREVIOUSLY_ELEVATED_FIELD,
        ] = (
            (
                df.loc[
                    (state,),
                    TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_LOWER_THAN_THRESHOLD_FIELD,
                ]
                == 0
            ).cumsum()
            > 0
        ).values

        # To be true on 1D, the state must be (1) lower than the threshold, AND (2) previously above the threshold.
        df.loc[(state,), CDC_CRITERIA_1D_COVID_NEAR_ZERO_INCIDENCE] = (
            df.loc[
                (state,),
                TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_PREVIOUSLY_ELEVATED_FIELD,
            ]
            & df.loc[
                (state,),
                TOTAL_NEW_CASES_IN_14_DAY_WINDOW_PER_100_K_POPULATION_LOWER_THAN_THRESHOLD_FIELD,
            ]
        ).values

        # Calculate all of the criteria combined in category 1.
        df.loc[(state,), CDC_CRITERIA_1_COMBINED_FIELD] = (
            (
                df.loc[(state,), CDC_CRITERIA_1A_COVID_CONTINUOUS_DECLINE_FIELD]
                & df.loc[(state,), CDC_CRITERIA_1B_COVID_NO_REBOUNDS_FIELD]
                & df.loc[(state,), CDC_CRITERIA_1C_COVID_OVERALL_DECLINE_FIELD]
            )
            | df.loc[(state,), CDC_CRITERIA_1D_COVID_NEAR_ZERO_INCIDENCE]
        ).values

        ###### Calculate criteria category 2. ######
        # For the criteria, we must add positive to negative tests to get the total (discarding inconclusive).
        df.loc[(state,), NEW_TESTS_TOTAL_FIELD] = (
            df.loc[(state,), NEW_CASES_POSITIVE_SOURCE_FIELD]
            + df.loc[(state,), NEW_CASES_NEGATIVE_SOURCE_FIELD]
        ).values

        df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_FIELD] = (
            df.loc[(state,), NEW_CASES_POSITIVE_SOURCE_FIELD].astype(float)
            / df.loc[(state,), NEW_TESTS_TOTAL_FIELD]
        ).values

        df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_DIFF_FIELD] = (
            df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_FIELD].diff(periods=1)
        ).values

        # Calculate 2A: Achieve 14 or more consecutive days of decline in percent positive ... with up to 2-3
        # consecutive days of increasing or stable percent positive allowed as a grace period if data are inconsistent.
        df.loc[
            (state,), MAX_RUN_OF_DECREASING_PERCENT_POSITIVE_TESTS_FIELD
        ] = get_max_run_in_window(
            series_=df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_DIFF_FIELD],
            window_size=14,
            positive_values=False,
        ).values

        df.loc[
            (state,), MAX_RUN_OF_INCREASING_PERCENT_POSITIVE_TESTS_FIELD
        ] = get_max_run_in_window(
            series_=df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_DIFF_FIELD],
            window_size=14,
            positive_values=True,
        ).values

        df.loc[(state,), CDC_CRITERIA_2A_COVID_PERCENT_CONTINUOUS_DECLINE_FIELD] = (
            df.loc[(state,), MAX_RUN_OF_DECREASING_PERCENT_POSITIVE_TESTS_FIELD] >= 11
        ).values

        # Calculate 2B: Total test volume is stable or increasing.
        df.loc[(state,), NEW_TESTS_TOTAL_DIFF_FIELD] = (
            df.loc[(state,), NEW_TESTS_TOTAL_FIELD].diff(periods=1).values
        )

        df.loc[
            (state,), MAX_RUN_OF_INCREASING_TOTAL_TESTS_FIELD
        ] = get_max_run_in_window(
            series_=df.loc[(state,), NEW_TESTS_TOTAL_DIFF_FIELD],
            window_size=14,
            positive_values=False,
        ).values

        df.loc[(state,), CDC_CRITERIA_2B_COVID_TOTAL_TEST_VOLUME_INCREASING_FIELD] = (
            df.loc[
                (state,),
                # TODO: is this the correct specification?
                MAX_RUN_OF_INCREASING_TOTAL_TESTS_FIELD,
            ]
            >= 11
        ).values

        # Calculate 2C: 14th day [of positive percentage of tests] must be lower than 1st day.
        df.loc[(state,), CDC_CRITERIA_2C_COVID_PERCENT_OVERALL_DECLINE_FIELD] = (
            df.loc[(state,), PERCENT_POSITIVE_NEW_TESTS_FIELD].diff(periods=14) < 0
        ).values

        # Calculate all of the criteria combined in category 2.
        df.loc[(state,), CDC_CRITERIA_2_COMBINED_FIELD] = (
            df.loc[(state,), CDC_CRITERIA_2A_COVID_PERCENT_CONTINUOUS_DECLINE_FIELD]
            & df.loc[(state,), CDC_CRITERIA_2B_COVID_TOTAL_TEST_VOLUME_INCREASING_FIELD]
            & df.loc[(state,), CDC_CRITERIA_2C_COVID_PERCENT_OVERALL_DECLINE_FIELD]
        ).values

        # Calculate all of the criteria combined.
        df.loc[(state,), CDC_CRITERIA_ALL_COMBINED_FIELD] = (
            df.loc[(state,), CDC_CRITERIA_1_COMBINED_FIELD]
            & df.loc[(state,), CDC_CRITERIA_2_COMBINED_FIELD]
        ).values

    # Add an update time.
    df[LAST_UPDATED_FIELD] = datetime.datetime.now()

    # Remove the multi-index, converting date and state back to just columns.
    df = df.reset_index(drop=False)

    return df


def get_consecutive_positive_or_negative_values(series_, positive_values=True):
    meets_criteria = series_ >= 0 if positive_values else series_ < 0
    consecutive_positive_values = meets_criteria * (
        meets_criteria.groupby(
            (meets_criteria != meets_criteria.shift()).cumsum()
        ).cumcount()
        + 1
    )
    return consecutive_positive_values


def get_max_run_in_window(series_, positive_values, window_size=14):
    # Assert that the index is sorted.
    if not series_.index.is_monotonic_increasing:
        raise ValueError("Index is not sorted.")

    returned_series = pd.Series(index=series_.index, data=np.nan)

    for i in range(window_size - 1, len(series_)):
        # Start calculating the run of values that happened *within (and only within)* this window.
        # TODO(lbrown): this is incredibly inefficient, but I can't think of a faster way while following the right
        #  interpretation of the rules.
        consecutive_positive_or_negative_values = get_consecutive_positive_or_negative_values(
            series_=series_.iloc[i + 1 - window_size : i + 1],
            positive_values=positive_values,
        )

        # Find the max run.
        returned_series[i] = consecutive_positive_or_negative_values.max()

    return returned_series


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
