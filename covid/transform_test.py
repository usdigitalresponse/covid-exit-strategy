import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from covid.transform import transform_county_data


class TransformTest(unittest.TestCase):
    @unittest.skip("Lots of formatting to fix this, TODO (@patricksheehan)")
    def test_transform_county_data(self):
        covidatlas_df = pd.read_csv("data/covidatlas_example_subset.csv")
        county_df = transform_county_data(covidatlas_df=covidatlas_df)
        expected_county_df = pd.read_csv("data/expected_county_df_example.csv",)
        assert_frame_equal(
            county_df, expected_county_df, check_dtype=False,
        )
