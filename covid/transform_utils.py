import datetime
from functools import partial
from operator import is_not

import numpy as np
import pandas as pd
from rpy2 import robjects as robjects
from scipy import interpolate as interpolate

from covid.extract import DATE_SOURCE_FIELD
from covid.extract import STATE_FIELD


def _filter_not_none(values):
    """Filters the passed values for ones that are not none.

    Args:
        values (list): values to filter

    Returns:
        filtered_values (list): filtered values
    """
    filtered_values = list(filter(partial(is_not, None), values))
    return filtered_values


def _reindex_and_rename_columns(frame, index, columns):
    """Reindexes and renames the columns of the passed data frame.

    Args:
        frame (pandas.DataFrame): data frame to reindex and rename columns of
        index (pandas.DatetimeIndex): index to use for reindexing
        columns (list): columns to use for renaming

    Returns:
        result_frame (pandas.DataFrame): resulting data frame
    """
    result_frame = pd.DataFrame(
        index=frame.index, columns=columns, data=frame.values
    ).reindex(index)
    return result_frame


def _create_period_column_names(
    columns, suffix, period, suffix_level, input_frame_cols
):
    """Helper method for compute_lagged_frame and compute_diff_frame that attaches suffixes to a column level.

    Args:
        columns (list of str or list of tuple of str): new columns - list of str for Index or list of tuple for
            MultiIndex
        suffix (str): suffix to add to the columns
        period (int): amount of lag/differencing to add to columns
        suffix_level (int): if MultiIndex, level to add the suffixes to
        input_frame_cols (pd.Index or pd.MultiIndex): the columns of the original input frame

    Returns:
        new_columns: the newly formatted columns for the output frame
    """
    # Check if the original frame had a MultiIndex or not
    if isinstance(input_frame_cols, pd.MultiIndex):
        new_columns = [list(col) for col in columns]

        # Add the suffix and period to the specified level of the MultiIndex
        new_columns = [
            col[0:suffix_level]
            + ["".join(map(str, _filter_not_none([col[suffix_level], suffix, period])))]
            + (col[suffix_level + 1 :] if suffix_level != -1 else [])
            for col in new_columns
        ]

        # Create new MultiIndex
        new_columns = [tuple(col) for col in new_columns]
        new_columns = pd.MultiIndex.from_tuples(
            new_columns, names=input_frame_cols.names
        )
    else:
        # Create a new list of columns with the suffix attached
        new_columns = [
            "".join(map(str, _filter_not_none([col, suffix, period])))
            for col in columns
        ]
    return new_columns


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

    r_x = robjects.DateVector(series_.index.get_level_values(DATE_SOURCE_FIELD))
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
    by_column=STATE_FIELD,
    num_lags=121,
    lag_timedelta=datetime.timedelta(days=1),
):
    # TODO(lbrown): Refactor this method to be more efficient; this is just the quick and dirty way.
    locations = df[by_column].unique()

    column_names = [DATE_SOURCE_FIELD]

    (
        column_name_formatter,
        lag_column_names,
    ) = generate_lag_column_name_formatter_and_column_names(
        column_name=column, num_lags=num_lags
    )

    column_names.extend(lag_column_names)

    lags_df = pd.DataFrame(
        index=pd.Index(data=locations, name=by_column), columns=column_names
    )

    today = pd.to_datetime(df[DATE_SOURCE_FIELD]).max()
    lags_df[DATE_SOURCE_FIELD] = today
    for location in locations:
        # Start each state looking up today.
        date_to_lookup = today

        for lag in range(num_lags):
            print(f"For field {column}, processing {location} for lag {lag}.")
            # Lookup the historical entry.
            value = df.loc[
                (df[by_column] == location) & (df[DATE_SOURCE_FIELD] == date_to_lookup),
                column,
            ]

            if len(value) > 1:
                raise ValueError("Too many or too few values returned.")
            elif len(value) == 1:
                value = value.iloc[0]
                lags_df.loc[location, column_name_formatter.format(lag)] = value

            date_to_lookup = date_to_lookup - lag_timedelta

    lags_df = lags_df.reset_index()
    return lags_df


def compute_lagged_frame(
    frame, num_periods, freq=None, suffix=None, suffix_level=None, subset=None
):
    """Computes a data frame with columns that contain the lagged values for the existing columns.

    Args:
        frame (pandas.DataFrame): data frame to compute the lagged values for
        num_periods (list of int): number of periods that each new feature column should be lagged by
        freq (str, optional): time frequency to use in the shift (e.g. 'H' for hourly)
        suffix (str, optional): suffix to use for titles of the new feature columns
        suffix_level (int, optional): if columns are MultiIndex, which level to add the suffix to
        subset (list of str, optional): subset of columns to compute lagged values for

    Returns:
        lagged_frame (pandas.DataFrame): data frame with the lagged values
    """
    if subset is None:
        subset = frame.columns.tolist()

    if suffix_level is None or suffix_level == len(frame.columns.levels):
        suffix_level = -1

    lagged_frame = pd.concat(
        [
            _reindex_and_rename_columns(
                frame=frame[subset].shift(periods, freq=freq),
                index=frame.index,
                columns=_create_period_column_names(
                    subset, suffix, periods, suffix_level, frame.columns
                ),
            )
            for periods in num_periods
        ],
        axis=1,
    )
    return lagged_frame


def calculate_summary(transformed_df, columns):
    # Find current date, and drop all other rows.
    current_date = transformed_df.loc[:, DATE_SOURCE_FIELD].max()

    summary_df = transformed_df.copy()
    summary_df = summary_df.loc[summary_df[DATE_SOURCE_FIELD] == current_date, columns]

    return summary_df


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
