"""
test_do_session.py
Author: Rajesh Kumar
~~~~~~~~~~~~~~~~~~

This module contains the unit test cases for the sessionization task
"""

from unittest import TestCase

from pyspark.sql import SparkSession
from do_session import calculate
from jobs.utils import read_data


class SessionizationTest(TestCase):
    """Test suite for sessionization in do_session.py
    """
    spark = None
    test_data_path = 'test_data/'

    @classmethod
    def setUpClass(cls) -> None:
        """Setup spark session
         and pre-requisite variables
        """
        cls.spark = SparkSession.builder \
            .master('local[3]') \
            .appName('SessionizationTest') \
            .getOrCreate()

    def test_read_data(self):
        """Test for read the data
        file
        """

        # assemble
        input_data = read_data(self.spark, self.test_data_path + 'download_logs.csv')

        # act
        input_data_cnt = input_data.count()

        # assert
        self.assertEqual(input_data_cnt, 11, "Record count should be 11")

    def test_do_session(self):
        """Test data sessionizer.

        Using small chuck of input data and expected output data, we
        test the sessionization task to make sure it's working as
        expected.
        """

        # assemble
        input_data = read_data(self.spark, self.test_data_path + 'download_logs.csv')
        test_size, test_cnt = calculate(self.spark, input_data)

        # act
        test_size_cnt = test_size.count()
        test_cnt_cnt = test_cnt.count()

        size_dict = dict()
        for row in test_size.collect():
            size_dict[row["session_id"]] = row["cum_size"]

        cnt_dict = dict()
        for row in test_cnt.collect():
            cnt_dict[row["session_id"]] = row["cnt"]

        # assert
        self.assertEqual(test_size_cnt, 1, "Record count should be 1")
        self.assertEqual(test_cnt_cnt, 1, "Record count should be 1")
        self.assertEqual(size_dict["107.23.85.jfd--S0"], 31923.0, "Total size of doc downloaded by (107.23.85.jfd--S0)")
        self.assertEqual(cnt_dict["107.23.85.jfd--S0"], 11, "Total count of doc downloaded by (107.23.85.jfd--S0)")

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop Spark
        """
        cls.spark.stop()
