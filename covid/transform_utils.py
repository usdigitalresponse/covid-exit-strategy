import datetime
import logging

import numpy as np
import pandas as pd
from rpy2 import robjects as robjects
from scipy import interpolate as interpolate

from covid.extract import DATE_SOURCE_FIELD
from covid.extract import STATE_SOURCE_FIELD


logger = logging.getLogger(__name__)


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


def get_consecutive_positive_or_negative_values(series_, positive_values=True):
    meets_criteria = series_ > 0 if positive_values else series_ < 0
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


def generate_lag_column_name_formatter_and_column_names(column_name, num_lags=121):
    column_name_formatter = f"{column_name}" + " T-{}"
    lag_column_names = [column_name_formatter.format(lag) for lag in range(num_lags)]
    # Put them in chronological order.
    lag_column_names.reverse()

    return column_name_formatter, lag_column_names


def generate_lags(df, column, num_lags=121):
    # TODO(lbrown): Refactor this method to be more efficient; this is just the quick and dirty way.
    states = df[STATE_SOURCE_FIELD].unique()

    column_names = [DATE_SOURCE_FIELD]

    (
        column_name_formatter,
        lag_column_names,
    ) = generate_lag_column_name_formatter_and_column_names(
        column_name=column, num_lags=num_lags
    )

    column_names.extend(lag_column_names)

    lags_df = pd.DataFrame(
        index=pd.Index(data=states, name=STATE_SOURCE_FIELD), columns=column_names
    )

    today = pd.to_datetime(df[DATE_SOURCE_FIELD]).max()
    lags_df[DATE_SOURCE_FIELD] = today
    for state in states:
        # Start each state looking up today.
        date_to_lookup = today

        for lag in range(num_lags):
            logger.info(f"For field {column}, processing {state} for lag {lag}.")
            # Lookup the historical entry.
            value = df.loc[
                (df[STATE_SOURCE_FIELD] == state)
                & (df[DATE_SOURCE_FIELD] == date_to_lookup),
                column,
            ]

            if len(value) > 1:
                raise ValueError("Too many or too few values returned.")
            elif len(value) == 1:
                value = value.iloc[0]
                lags_df.loc[state, column_name_formatter.format(lag)] = value

            date_to_lookup = date_to_lookup - datetime.timedelta(days=1)

    lags_df = lags_df.reset_index()
    return lags_df


def calculate_state_summary(transformed_df, columns):
    # Find current date, and drop all other rows.
    current_date = transformed_df.loc[:, DATE_SOURCE_FIELD].max()

    state_summary_df = transformed_df.copy()
    state_summary_df = state_summary_df.loc[
        state_summary_df[DATE_SOURCE_FIELD] == current_date, columns
    ]

    return state_summary_df
