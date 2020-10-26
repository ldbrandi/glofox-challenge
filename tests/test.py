import logging
import unittest
import pandas as pd

from pyspark.sql import SparkSession

class GlofoxTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
            .master('local[*]')
            .appName('testing-glofox')
            .enableHiveSupport()
            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class SimpleTest(GlofoxTest):

    def test_basic(self):
        from operator import add

        test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
        results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
        self.assertEqual(set(results), set(expected_results))

    def my_spark_function(df):
        return df.filter('year' >= 2000)

    def test_data_frame(self):
        # Create the test data, with larger examples this can come from a CSV file
        # and we can use pd.read_csv(…)
        data_pandas = pd.DataFrame({'make':['Jaguar', 'MG', 'MINI', 'Rover', 'Lotus'],
        'registration':['AB98ABCD','BC99BCDF','CD00CDE','DE01DEF','EF02EFG'],
        'year':[1998,1999,2000,2001,2002]})
        # Turn the data into a Spark DataFrame, self.spark comes from our PySparkTest base class
        data_spark = self.spark.createDataFrame(data_pandas)
        # Invoke the unit we'd like to test
        results_spark = my_spark_function(data_spark)
        # Turn the results back to Pandas
        results_pandas = results_spark.toPandas()
        # Our expected results crafted by hand, again, this could come from a CSV
        # in case of a bigger example
        expected_results = pd.DataFrame({'make':['Rover', 'Lotus', 'MINI'],
        'registration':['DE01DEF','EF02EFG', 'CD00CDE'],
        'year':[2001,2002, 2000]})
        # Assert that the 2 results are the same. We'll cover this function in a bit
        pd.assert_frame_equal_with_sort(results_pandas, expected_results, ['registration'])

    def assert_frame_equal_with_sort(results, expected, keycolumns):
        results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
        pd.assert_frame_equal(results_sorted, expected_sorted)