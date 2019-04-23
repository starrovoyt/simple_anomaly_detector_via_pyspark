import pandas as pd

from functools import partial

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


class AnomalyDetector(object):
    def __init__(self, properties_df, anomaly_rate, training_sample_rate, with_scaling=False):
        self.properties_df = properties_df
        self.training_sample_rate = training_sample_rate
        self.model = IsolationForest(contamination=anomaly_rate)
        if with_scaling:
            self.scaler = StandardScaler()
        else:
            self.scaler = None

    def get_training_batch(self):
        return self.properties_df.sample(False, self.training_sample_rate)

    def train(self):
        training_df = self.get_training_batch().toPandas()
        pandas_data = training_df[['frequency', 'alternating', 'periodicity', 'repetitions', 'neighbours']]

        if self.scaler is None:
            norm_data = pandas_data
        else:
            norm_data = self.scaler.fit_transform(pandas_data)

        self.model.fit(norm_data)

    @staticmethod
    def prediction_mapper(row, scaler, model):
        pandas_row_data = pd.DataFrame([{'frequency': row.frequency,
                                         'alternating': row.alternating,
                                         'periodicity': row.periodicity,
                                         'repetitions': row.repetitions,
                                         'neighbours': row.neighbours}])
        if scaler is None:
            norm_data = pandas_row_data
        else:
            norm_data = scaler.transform(pandas_row_data)
        predicted_data = model.score_samples(norm_data)
        return {
            'anomaly_proba': float(1. - (predicted_data[0] + 1.) / 2.),
            'id': row.id,
            'id_type': row.id_type
        }

    def make_full_prediction(self):
        score_df = self.properties_df.rdd.map(partial(AnomalyDetector.prediction_mapper,
                                                      scaler=self.scaler,
                                                      model=self.model)).toDF()
        return score_df
