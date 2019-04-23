from anomaly_detection import config
from anomaly_detection.anomaly_detector import AnomalyDetector
from util.utils import SQLHelper

from anomaly_detection.config import SparkConfig
from anomaly_detection.config import DataBase
from anomaly_detection.config import SQLQuery
from anomaly_detection.identifier import Identifier


def count_neighbours(id_type, edges_df, ss):
    if ss is None:
        raise Exception('Need spark client')

    edges_df_name = 'edges_table'
    edges_df.registerTempTable(edges_df_name)

    query = SQLHelper.render_query(
        SQLQuery.NEIGHBOURS_COUNT,
        edges_table=edges_df_name,
        id_type=id_type)

    neighbours_df = ss.sql(query).toDF('id', 'id_type', 'neighbours_number')
    return neighbours_df


def create_df_with_properties(neighbours_df):
    new_df = neighbours_df.rdd.map(map_properties).toDF()
    return new_df


def map_properties(row):
    id_value = row.id
    id_type = row.id_type
    neighbours_number = row.neighbours_number
    identifier = Identifier(id_value, id_type)
    properties_row = {'id': id_value,
                      'id_type': id_type,
                      'frequency': float(identifier.frequency),
                      'alternating': float(identifier.alternating),
                      'periodicity': float(identifier.periodicity),
                      'repetitions': float(identifier.repetitions),
                      'neighbours': float(neighbours_number)}
    return properties_row


def main():
    edges_df = SparkConfig.ss.read.csv(DataBase.EDGES_TABLE_PATH,
                                       header=True,
                                       schema=DataBase.EDGES_TABLE_SCHEMA)

    neighbours_df = count_neighbours(config.ID_TYPE, edges_df=edges_df, ss=SparkConfig.ss)
    neighbours_df.write.csv(DataBase.NEIGHBOURS_TABLE_PATH)

    properties_df = create_df_with_properties(neighbours_df=neighbours_df)
    properties_df.write.csv(DataBase.PROPERTIES_TABLE_PATH)

    detector = AnomalyDetector(properties_df, anomaly_rate=0.1, training_sample_rate=0.5)
    detector.train()
    anomaly_df = detector.make_full_prediction()
    anomaly_df.write.csv(DataBase.OUTLIERS_TABLE_PATH)

    anomaly_df.show()
    SparkConfig.ss.stop()


if __name__ == '__main__':
    main()
