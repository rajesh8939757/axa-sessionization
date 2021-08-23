"""
test_do_session.py
Author: Rajesh Kumar
~~~~~~~~~~~~~~~~~~

This module contains the unit test cases for the sessionization task
"""

import unittest

from dependecies.spark import start_spark
from jobs.do_session import calculate


class SessionizationTest(unittest.TestCase):
    """Test suite for sessionization in do_session.py
    """

    def setUp(self):
        """Start spark, define config and path to test data
        """

        self.spark, *_ = start_spark()
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_do_session(self):
        """Test data sessionizer.

        Using small chuck of input data and expected output data, we
        test the sessionization task to make sure it's working as
        expected.
        """

        # assemble
        input_data = (
            self.spark
            .read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv(self.test_data_path+'download_logs.csv')
        )

        expected_size = (
            self.spark
                .read
                .option('header', 'true')
                .option('inferSchema', 'true')
                .csv(self.test_data_path + 'test_size_agg.csv')
        )

        expected_cnt = (
            self.spark
                .read
                .option('header', 'true')
                .option('inferSchema', 'true')
                .csv(self.test_data_path + 'test_cnt_agg.csv')
        )

        expected_cols_cnt = [len(expected_size.columns), len(expected_cnt.columns)]
        expected_rows_cnt = [expected_size.count(), expected_cnt.count()]

        expected_size_value = expected_size.select('cum_size').collect()[0]['cum_size']
        expected_cnt_value = expected_cnt.select('cnt').collect()[0]['cnt']

        # act
        test_size, test_cnt = calculate(self.spark, input_data)

        cols = [len(test_size.columns), len(test_cnt.columns)]
        rows = [test_size.count(), test_cnt.count()]

        test_size_value = test_size.select('cum_size').collect()[0]['cum_size']
        test_cnt_value = test_cnt.select('cnt').collect()[0]['cnt']

        # assert
        self.assertEqual(expected_cols_cnt, cols)
        self.assertEqual(expected_rows_cnt, rows)

        self.assertEqual(expected_size_value, test_size_value)
        self.assertEqual(expected_cnt_value, test_cnt_value)

        self.assertTrue([col in expected_size.columns
                         for col in test_size.columns])
        self.assertTrue([col in expected_cnt.columns
                         for col in test_cnt.columns])


if __name__ == '__main__':
    unittest.main()
