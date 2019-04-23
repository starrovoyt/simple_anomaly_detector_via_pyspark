import os

import pyspark

from anomaly_detection.id_types import IdType

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, FloatType

ID_TYPE = IdType.LOGIN


class SparkConfig:
    sc = pyspark.SparkContext(master=os.environ['SPARK_MASTER'], appName='anomaly_detector')
    ss = SparkSession(sc)


class DataBase:
    DATA_DIR = os.environ.get('DATA_DIR')
    EDGES_TABLE_PATH = os.path.join(DATA_DIR, os.environ.get('EDGES_TABLE_NAME'))
    NEIGHBOURS_TABLE_PATH = os.path.join(DATA_DIR, 'neighbours_number_{}.csv'.format(ID_TYPE))
    PROPERTIES_TABLE_PATH = os.path.join(DATA_DIR, 'properties_{}.csv'.format(ID_TYPE))
    OUTLIERS_TABLE_PATH = os.path.join(DATA_DIR, 'anomaly_probas_{}.csv'.format(ID_TYPE))

    EDGES_TABLE_SCHEMA = StructType([
        StructField('id', StringType(), True),
        StructField('id_type', StringType(), True),
        StructField('neighbour_id', StringType(), True),
        StructField('neighbour_id_type', StringType(), True)
    ])

    NEIGHBOURS_TABLE_SCHEMA = StructType([
        StructField('id', StringType(), True),
        StructField('id_type', StringType(), True),
        StructField('neighbours_number', StringType(), True),
    ])

    PROPERTIES_TABLE_SCHEMA = StructType([
        StructField('id', StringType(), True),
        StructField('id_type', StringType(), True),
        StructField('frequency', FloatType(), True),
        StructField('alternating', FloatType(), True),
        StructField('periodicity', FloatType(), True),
        StructField('repetitions', FloatType(), True),
        StructField('neighbours', FloatType(), True)
    ])

    OUTLIERS_TABLE_SCHEMA = StructType([
        StructField('id', StringType(), True),
        StructField('id_type', StringType(), True),
        StructField('anomaly_proba', FloatType(), True),
    ])


# TODO: fix paths
class SQLQuery(object):
    NEIGHBOURS_COUNT = '/anomaly_detection/sql/neighbours_count.sql'
    GET_NEIGHBOURS_NUMBER = '/anomaly_detection/sql/get_neighbours_number.sql'
