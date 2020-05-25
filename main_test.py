import datetime
import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from main import find_consecutive_positive_or_negative_values
from main import fit_and_predict_cubic_spline


class MainTest(unittest.TestCase):
    def test_find_consecutive_positive_values(self):
        assert_series_equal(
            find_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, 1, 2, -4, 3, 5]), positive_values=True
            ),
            pd.Series([0, 1, 2, 0, 1, 2]),
        )

        assert_series_equal(
            find_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, -1, 0, 1, 2, 3, -15, -17.0]),
                positive_values=True,
            ),
            pd.Series([0, 0, 1, 2, 3, 4, 0, 0]),
        )

        # Make sure we handle null values appropriately.
        assert_series_equal(
            find_consecutive_positive_or_negative_values(
                series_=pd.Series([np.nan, 1, 2, -4, np.nan, 5]), positive_values=True
            ),
            pd.Series([0, 1, 2, 0, 0, 1]),
        )

        # Make sure we handle null values appropriately.
        assert_series_equal(
            find_consecutive_positive_or_negative_values(
                series_=pd.Series([np.nan, np.nan, 2, 4, np.nan, 5]),
                positive_values=True,
            ),
            pd.Series([0, 0, 1, 2, 0, 1]),
        )

        # Now, test for negative values.
        assert_series_equal(
            find_consecutive_positive_or_negative_values(
                series_=pd.Series([-1, 1, 2, -4, 3, 5]), positive_values=False
            ),
            pd.Series([1, 0, 0, 1, 0, 0]),
        )

        assert_series_equal(
            find_consecutive_positive_or_negative_values(
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
