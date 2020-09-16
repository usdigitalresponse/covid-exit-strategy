import datetime

import numpy as np
import pandas as pd
from rpy2 import robjects as robjects
from scipy import interpolate as interpolate

from covid.extract import DATE_SOURCE_FIELD
from covid.extract import STATE_FIELD


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


def calculate_consecutive_positive_or_negative_values(series_, positive_values=True):
    meets_criteria = series_ > 0 if positive_values else series_ < 0
    consecutive_positive_values = meets_criteria * (
        meets_criteria.groupby(
            (meets_criteria != meets_criteria.shift()).cumsum()
        ).cumcount()
        + 1
    )
    return consecutive_positive_values


def calculate_max_run_in_window(series_, positive_values, window_size=14):
    # Assert that the index is sorted.
    if not series_.index.is_monotonic_increasing:
        raise ValueError("Index is not sorted.")

    returned_series = pd.Series(index=series_.index, data=np.nan)

    for i in range(window_size - 1, len(series_)):
        # Start calculating the run of values that happened *within (and only within)* this window.
        # TODO(lbrown): this is incredibly inefficient, but I can't think of a faster way while following the right
        #  interpretation of the rules.
        consecutive_positive_or_negative_values = calculate_consecutive_positive_or_negative_values(
            series_=series_.iloc[i + 1 - window_size : i + 1],
            positive_values=positive_values,
        )

        # Find the max run.
        returned_series[i] = consecutive_positive_or_negative_values.max()

    return returned_series


def generate_lag_column_name_formatter_and_column_names(column_name, num_lags=121):
    column_name_formatter = f"{column_name}" + " T-{}"
    lag_column_names = [column_name_formatter.format(lag) for lag in range(num_lags)]
    # Put them in chronological order.
    lag_column_names.reverse()

    return column_name_formatter, lag_column_names


def generate_lags(
    df,
    column,
    num_lags=121,
    lag_timedelta=datetime.timedelta(days=1),
    suffix_with_date=False,
    date_format="%Y-%m-%d",
):
    # TODO(lbrown): Refactor this method to be more efficient; this is just the quick and dirty way.
    states = df[STATE_FIELD].unique()

    # Create an empty dataframe to populate with lags.
    lags_df = pd.DataFrame(
        index=pd.Index(data=states, name=STATE_FIELD), columns=[DATE_SOURCE_FIELD]
    )

    # Calculate the latest date in the given data frame.
    latest_date = pd.to_datetime(df[DATE_SOURCE_FIELD]).max()
    lags_df[DATE_SOURCE_FIELD] = latest_date

    for state in states:
        # Start each state looking up the latest date we have.
        current_date = latest_date

        for current_lag in range(num_lags):
            print(f"For field {column}, processing {state} for lag {current_lag}.")

            # Lookup the historical entry.
            value = df.loc[
                (df[STATE_FIELD] == state)
                & (pd.to_datetime(df[DATE_SOURCE_FIELD]) == current_date),
                column,
            ]

            # Ensure only one value is returned.
            if len(value) > 1:
                raise ValueError("Too many values returned.")
            elif len(value) == 1:
                value = value.iloc[0]
            else:
                value = None

            if suffix_with_date:
                current_lag_column = f"{column}-{current_date.strftime(date_format)}"
            else:
                current_lag_column = f"{column} T-{current_lag}"

            # Add the current lag column.
            lags_df.loc[state, current_lag_column] = value

            # Decrement the current date by the lag amount.
            current_date = current_date - lag_timedelta

    lags_df = lags_df.reset_index()
    return lags_df


def calculate_state_summary(transformed_df, columns=None):
    # Find current date, and drop all other rows.
    current_date = transformed_df.loc[:, DATE_SOURCE_FIELD].max()

    state_summary_df = transformed_df.copy()
    state_summary_df = state_summary_df.loc[
        state_summary_df[DATE_SOURCE_FIELD] == current_date, :
    ]

    if columns is not None:
        state_summary_df = state_summary_df.loc[:, columns]

    return state_summary_df


def calculate_consecutive_boolean_series(boolean_series):
    """Calculates the number of consecutive booleans (`True` / `False`) in the given boolean series."""
    consecutive_true_series = calculate_consecutive_positive_or_negative_values(
        series_=(boolean_series.astype(bool).replace({False: -1, True: 1})),
        positive_values=True,
    )

    consecutive_false_series = calculate_consecutive_positive_or_negative_values(
        series_=(boolean_series.astype(bool).replace({False: -1, True: 1})),
        positive_values=False,
    )

    return consecutive_true_series, consecutive_false_series
