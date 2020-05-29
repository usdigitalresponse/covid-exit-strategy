import numpy as np
import pandas as pd
from rpy2 import robjects as robjects
from scipy import interpolate as interpolate


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
