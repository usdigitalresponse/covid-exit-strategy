import datetime
import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from main import fit_and_predict_cubic_spline
from main import fit_and_predict_cubic_spline_in_r
from main import get_consecutive_positive_or_negative_values
from main import get_max_run_in_window


class MainTest(unittest.TestCase):
    def test_find_consecutive_positive_values(self):
        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, 1, 2, -4, 3, 5]), positive_values=True
            ),
            pd.Series([0, 1, 2, 0, 1, 2]),
        )

        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, -1, 0, 1, 2, 3, -15, -17.0]),
                positive_values=True,
            ),
            pd.Series([0, 0, 1, 2, 3, 4, 0, 0]),
        )

        # Make sure we handle null values appropriately.
        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([np.nan, 1, 2, -4, np.nan, 5]), positive_values=True
            ),
            pd.Series([0, 1, 2, 0, 0, 1]),
        )

        # Make sure we handle null values appropriately.
        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([np.nan, np.nan, 2, 4, np.nan, 5]),
                positive_values=True,
            ),
            pd.Series([0, 0, 1, 2, 0, 1]),
        )

        # Now, test for negative values.
        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, 1, 2, -4, 3, 5]), positive_values=False
            ),
            pd.Series([1, 0, 0, 1, 0, 0]),
        )

        assert_series_equal(
            get_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, -1, -2, 0, -7, 5]), positive_values=False
            ),
            pd.Series([1, 2, 3, 0, 1, 0]),
        )

    def test_fit_and_predict_cubic_spline(self):
        assert_series_equal(
            fit_and_predict_cubic_spline(
                pd.Series(
                    data=[-1, 1, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                )
            ),
            pd.Series(
                data=[
                    -1.12917523,
                    1.69569136,
                    0.50898684,
                    -2.40935642,
                    2.154863,
                    5.17899044,
                ],
                index=[
                    pd.to_datetime("2020-01-01"),
                    pd.to_datetime("2020-01-02"),
                    pd.to_datetime("2020-01-03"),
                    pd.to_datetime("2020-01-04"),
                    pd.to_datetime("2020-01-05"),
                    pd.to_datetime("2020-01-06"),
                ],
            ),
        )

        # Should raise errors:
        with self.assertRaises(ValueError):
            fit_and_predict_cubic_spline(
                pd.Series(
                    data=[-1, 1, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            # This timestamp is out of order.
                            pd.to_datetime("2020-01-01"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                )
            )

    def test_fit_and_predict_cubic_spline_in_r(self):
        # Test with default smoothing parameter.
        assert_series_equal(
            fit_and_predict_cubic_spline_in_r(
                pd.Series(
                    data=[-1, 1, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                )
            ),
            pd.Series(
                data=[
                    -1.1428568397306358,
                    -0.2857142936746852,
                    0.5714282888471683,
                    1.4285710563026406,
                    2.285714184172537,
                    3.1428575140383694,
                ],
                index=[
                    pd.to_datetime("2020-01-01"),
                    pd.to_datetime("2020-01-02"),
                    pd.to_datetime("2020-01-03"),
                    pd.to_datetime("2020-01-04"),
                    pd.to_datetime("2020-01-05"),
                    pd.to_datetime("2020-01-06"),
                ],
            ),
        )

        # Test with null values.
        assert_series_equal(
            fit_and_predict_cubic_spline_in_r(
                pd.Series(
                    data=[np.nan, np.nan, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
                replace_nan=True,
            ),
            pd.Series(
                data=[
                    -0.6854102994488032,
                    -0.21069773360008473,
                    0.33578816306613873,
                    1.067404321401707,
                    2.12766929964248,
                    3.365246248938551,
                ],
                index=[
                    pd.to_datetime("2020-01-01"),
                    pd.to_datetime("2020-01-02"),
                    pd.to_datetime("2020-01-03"),
                    pd.to_datetime("2020-01-04"),
                    pd.to_datetime("2020-01-05"),
                    pd.to_datetime("2020-01-06"),
                ],
            ),
        )

        # Test with explicit smoothing parameter.
        assert_series_equal(
            fit_and_predict_cubic_spline_in_r(
                series_=pd.Series(
                    data=[-1, 1, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
                smoothing_parameter=0.5,
            ),
            pd.Series(
                data=[
                    -0.5415075617582621,
                    -0.1765863921621331,
                    0.07061496437277474,
                    0.5778703926676914,
                    2.0462976989861517,
                    4.023310897893784,
                ],
                index=[
                    pd.to_datetime("2020-01-01"),
                    pd.to_datetime("2020-01-02"),
                    pd.to_datetime("2020-01-03"),
                    pd.to_datetime("2020-01-04"),
                    pd.to_datetime("2020-01-05"),
                    pd.to_datetime("2020-01-06"),
                ],
            ),
        )

        # Should raise errors due to the index out of order.
        with self.assertRaises(ValueError):
            fit_and_predict_cubic_spline_in_r(
                pd.Series(
                    data=[-1, 1, 2, -4, 3, 5],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            # This timestamp is out of order.
                            pd.to_datetime("2020-01-01"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                )
            )

    def test_get_max_run_in_window(self):
        # A very simple series.
        assert_series_equal(
            get_max_run_in_window(
                positive_values=True,
                window_size=3,
                series_=pd.Series(
                    data=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            pd.to_datetime("2020-01-07"),
                            pd.to_datetime("2020-01-08"),
                            pd.to_datetime("2020-01-09"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
            ),
            pd.Series(
                data=[np.nan, np.nan, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
                index=pd.DatetimeIndex(
                    data=[
                        pd.to_datetime("2020-01-01"),
                        pd.to_datetime("2020-01-02"),
                        pd.to_datetime("2020-01-03"),
                        pd.to_datetime("2020-01-04"),
                        pd.to_datetime("2020-01-05"),
                        pd.to_datetime("2020-01-06"),
                        pd.to_datetime("2020-01-07"),
                        pd.to_datetime("2020-01-08"),
                        pd.to_datetime("2020-01-09"),
                    ],
                    freq=datetime.timedelta(days=1),
                ),
            ),
        )

        assert_series_equal(
            get_max_run_in_window(
                positive_values=True,
                window_size=3,
                series_=pd.Series(
                    data=[1, 2, 3, -1, 1, 2, -1, -1, -3],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            pd.to_datetime("2020-01-07"),
                            pd.to_datetime("2020-01-08"),
                            pd.to_datetime("2020-01-09"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
            ),
            pd.Series(
                data=[np.nan, np.nan, 3.0, 2.0, 1.0, 2.0, 2.0, 1.0, 0],
                index=pd.DatetimeIndex(
                    data=[
                        pd.to_datetime("2020-01-01"),
                        pd.to_datetime("2020-01-02"),
                        pd.to_datetime("2020-01-03"),
                        pd.to_datetime("2020-01-04"),
                        pd.to_datetime("2020-01-05"),
                        pd.to_datetime("2020-01-06"),
                        pd.to_datetime("2020-01-07"),
                        pd.to_datetime("2020-01-08"),
                        pd.to_datetime("2020-01-09"),
                    ],
                    freq=datetime.timedelta(days=1),
                ),
            ),
        )

        assert_series_equal(
            get_max_run_in_window(
                positive_values=False,
                window_size=3,
                series_=pd.Series(
                    data=[1, 2, 3, -1, 1, 2, -1, -1, -3],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            pd.to_datetime("2020-01-07"),
                            pd.to_datetime("2020-01-08"),
                            pd.to_datetime("2020-01-09"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
            ),
            pd.Series(
                data=[np.nan, np.nan, 0.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0],
                index=pd.DatetimeIndex(
                    data=[
                        pd.to_datetime("2020-01-01"),
                        pd.to_datetime("2020-01-02"),
                        pd.to_datetime("2020-01-03"),
                        pd.to_datetime("2020-01-04"),
                        pd.to_datetime("2020-01-05"),
                        pd.to_datetime("2020-01-06"),
                        pd.to_datetime("2020-01-07"),
                        pd.to_datetime("2020-01-08"),
                        pd.to_datetime("2020-01-09"),
                    ],
                    freq=datetime.timedelta(days=1),
                ),
            ),
        )

        assert_series_equal(
            get_max_run_in_window(
                positive_values=True,
                series_=pd.Series(
                    data=[
                        18,
                        19,
                        -1,
                        -2,
                        -3,
                        1,
                        2,
                        3,
                        4,
                        5,
                        -1,
                        -19,
                        -3,
                        -2,
                        9,
                        2,
                        3,
                        4,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                    ],
                    index=pd.DatetimeIndex(
                        data=[
                            pd.to_datetime("2020-01-01"),
                            pd.to_datetime("2020-01-02"),
                            pd.to_datetime("2020-01-03"),
                            pd.to_datetime("2020-01-04"),
                            pd.to_datetime("2020-01-05"),
                            pd.to_datetime("2020-01-06"),
                            pd.to_datetime("2020-01-07"),
                            pd.to_datetime("2020-01-08"),
                            pd.to_datetime("2020-01-09"),
                            pd.to_datetime("2020-01-10"),
                            pd.to_datetime("2020-01-11"),
                            pd.to_datetime("2020-01-12"),
                            pd.to_datetime("2020-01-13"),
                            pd.to_datetime("2020-01-14"),
                            pd.to_datetime("2020-01-15"),
                            pd.to_datetime("2020-01-16"),
                            pd.to_datetime("2020-01-17"),
                            pd.to_datetime("2020-01-18"),
                            pd.to_datetime("2020-01-19"),
                            pd.to_datetime("2020-01-20"),
                            pd.to_datetime("2020-01-21"),
                            pd.to_datetime("2020-01-22"),
                            pd.to_datetime("2020-01-23"),
                            pd.to_datetime("2020-01-24"),
                            pd.to_datetime("2020-01-25"),
                            pd.to_datetime("2020-01-26"),
                            pd.to_datetime("2020-01-27"),
                            pd.to_datetime("2020-01-28"),
                        ],
                        freq=datetime.timedelta(days=1),
                    ),
                ),
            ),
            pd.Series(
                data=[
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    5.0,
                    5.0,
                    5.0,
                    5.0,
                    5.0,
                    5.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                    4.0,
                ],
                index=pd.DatetimeIndex(
                    data=[
                        pd.to_datetime("2020-01-01"),
                        pd.to_datetime("2020-01-02"),
                        pd.to_datetime("2020-01-03"),
                        pd.to_datetime("2020-01-04"),
                        pd.to_datetime("2020-01-05"),
                        pd.to_datetime("2020-01-06"),
                        pd.to_datetime("2020-01-07"),
                        pd.to_datetime("2020-01-08"),
                        pd.to_datetime("2020-01-09"),
                        pd.to_datetime("2020-01-10"),
                        pd.to_datetime("2020-01-11"),
                        pd.to_datetime("2020-01-12"),
                        pd.to_datetime("2020-01-13"),
                        pd.to_datetime("2020-01-14"),
                        pd.to_datetime("2020-01-15"),
                        pd.to_datetime("2020-01-16"),
                        pd.to_datetime("2020-01-17"),
                        pd.to_datetime("2020-01-18"),
                        pd.to_datetime("2020-01-19"),
                        pd.to_datetime("2020-01-20"),
                        pd.to_datetime("2020-01-21"),
                        pd.to_datetime("2020-01-22"),
                        pd.to_datetime("2020-01-23"),
                        pd.to_datetime("2020-01-24"),
                        pd.to_datetime("2020-01-25"),
                        pd.to_datetime("2020-01-26"),
                        pd.to_datetime("2020-01-27"),
                        pd.to_datetime("2020-01-28"),
                    ],
                    freq=datetime.timedelta(days=1),
                ),
            ),
        )
