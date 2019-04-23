import unittest

from pyspark.sql.types import StructType, StructField, StringType

from anomaly_detection.main import count_neighbours
from anomaly_detection.identifier import Identifier
from anomaly_detection.main import create_df_with_properties
from anomaly_detection.anomaly_detector import AnomalyDetector

from util.utils import IdentifierHelper

from anomaly_detection.config import SparkConfig


class Tester(unittest.TestCase):
    spark_context = SparkConfig.sc
    ss = SparkConfig.ss

    def test_device_id_pattern(self):
        pattern = IdentifierHelper.device_id_pattern
        matched = pattern.match('AAAAAAAA-AAAA-BBBB-0000-FFFFFFFFFFFFFFFF')
        self.assertIsNotNone(matched)

        matched = pattern.match('AAAAAAAA-AAAA-BBBB-0000-GGGGGGGGGGGGGGGG')
        self.assertIsNone(matched)

        matched = pattern.match('1234567890')
        self.assertIsNone(matched)

    def test_email_pattern(self):
        pattern = IdentifierHelper.email_pattern
        matched = pattern.match('p@shrg.ru')
        self.assertIsNotNone(matched)

        matched = pattern.match('ne-pshrg')
        self.assertIsNone(matched)

    def test_frequency_variance(self):
        var = IdentifierHelper.frequency_variance([1, 1, 1, 2], 5)
        self.assertAlmostEqual(var, 0.24, delta=0.01)

    def test_frequency(self):
        freq = Identifier.get_frequency(value='aaaa')
        self.assertAlmostEqual(freq, 0.0, delta=0.01)

        freq = Identifier.get_frequency(value='abcdd')
        self.assertAlmostEqual(freq, 0.24, delta=0.01)

    def test_alternating(self):
        alternating = Identifier.get_alternating('1234')
        self.assertAlmostEqual(alternating, 0.0, delta=0.01)

        alternating = Identifier.get_alternating('123444')
        self.assertAlmostEqual(alternating, 0.24, delta=0.01)

        alternating = Identifier.get_alternating('1212121')
        self.assertAlmostEqual(alternating, -1, delta=0.01)

    def test_period(self):
        period = Identifier.get_periodicity('123123123123')
        self.assertAlmostEqual(period, 24, delta=0.01)

        period = Identifier.get_periodicity('12345')
        self.assertAlmostEqual(period, 0, delta=0.01)

        period = Identifier.get_periodicity('123123ab123')
        self.assertAlmostEqual(period, 15, delta=0.01)

    def test_repetitions(self):
        repetitions = Identifier.get_repetitions('111111')
        self.assertAlmostEqual(repetitions, 10, delta=0.01)

        repetitions = Identifier.get_repetitions('1234567890')
        self.assertAlmostEqual(repetitions, 0, delta=0.01)

    def test_neighbours(self):
        neighbours = Identifier.get_neighbours('qwerty@yandex.ru', 'email')
        self.assertEqual(neighbours, 1)

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("neighbours_number", StringType(), True)
        ])

        df = Tester.ss.read.csv('test_data/test_neighbours_number_data.csv', header=True, schema=schema)
        df.show()

        res = Identifier.get_neighbours('asdasdasd', 'login', df, Tester.ss)
        self.assertEqual(res, 2)

        res = Identifier.get_neighbours('ghjk', 'other', df, Tester.ss)
        self.assertEqual(res, 10)

    def test_neighbours_count(self):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("neighbour_id", StringType(), True),
            StructField("neighbour_id_type", StringType(), True)
        ])

        df = Tester.ss.read.csv('test_data/test_neighbours_count_data.csv', header=True, schema=schema)
        res_df = count_neighbours(id_type='login', edges_df=df, ss=Tester.ss).toPandas()

        self.assertEqual(res_df.neighbours_number[0], 1)
        self.assertEqual(res_df.neighbours_number[6], 3)

    def test_neighbours_number(self):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("neighbours_number", StringType(), True)
        ])

        df = Tester.ss.read.csv('test_data/test_neighbours_number_data.csv', header=True, schema=schema)
        res_df = create_df_with_properties(neighbours_df=df).toPandas()

        self.assertAlmostEqual(res_df.alternating[0], -0.5, delta=0.01)
        self.assertAlmostEqual(res_df.frequency[1], 0.0, delta=0.01)
        self.assertAlmostEqual(res_df.frequency[1], 0.0, delta=0.01)
        self.assertAlmostEqual(res_df.neighbours[2], 1, delta=0.01)
        self.assertAlmostEqual(res_df.periodicity[1], 12, delta=0.01)
        self.assertAlmostEqual(res_df.repetitions[1], 15, delta=0.01)

    def test_detector(self):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("id_type", StringType(), True),
            StructField("neighbours_number", StringType(), True)
        ])

        df = Tester.ss.read.csv('test_data/test_neighbours_number_data.csv', header=True, schema=schema)
        properties_df = create_df_with_properties(neighbours_df=df)

        detector = AnomalyDetector(properties_df, 0.2, 0.9)
        detector.train()
        res_df = detector.make_full_prediction()
        res_df.show()


if __name__ == '__main__':
    unittest.main()
    Tester.ss.stop()
