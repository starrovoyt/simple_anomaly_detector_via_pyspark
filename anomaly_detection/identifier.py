import collections

import numpy as np

from util.utils import SQLHelper

from anomaly_detection.id_types import IdType
from util.utils import IdentifierHelper
from anomaly_detection.config import SQLQuery


class Identifier(object):
    def __init__(self, id_value, id_type, spark_session=None, edges_df=None, count_properties=True):
        self.value = id_value
        self.type = id_type

        self.normalize()

        self.cleared_value = id_value
        self.clear_value()

        if count_properties:
            self.frequency = Identifier.get_frequency(self.cleared_value)
            self.alternating = Identifier.get_alternating(self.cleared_value)
            self.periodicity = Identifier.get_periodicity(self.cleared_value)
            self.repetitions = Identifier.get_repetitions(self.cleared_value)
            self.neighbours = Identifier.get_neighbours(self.value, self.type, edges_df, spark_session)

        else:
            self.frequency = None
            self.alternating = None
            self.periodicity = None
            self.repetitions = None
            self.neighbours = None

    def normalize(self):
        if self.type in IdType.DEVICE_ID:
            self.value = self.value.upper()

    def validate(self):
        if self.type in IdType.DEVICE_ID:
            matched = IdentifierHelper.device_id_pattern(self.value)
            if not matched:
                raise ValueError('Id {} does not match to device_id pattern.'.format(self.value))
        elif self.type == IdType.EMAIL:
            matched = IdentifierHelper.email_pattern
            if not matched:
                raise ValueError('Id {} does not match to email pattern.'.format(self.value))
        else:
            if not self.type == IdType.LOGIN:
                raise ValueError('No such id type {}.'.format(self.type))

    def clear_value(self):
        if self.type in IdType.HUMAN_ID:
            self.cleared_value = self.value.split('@')[0]
        elif self.type in IdType.DEVICE_ID:
            self.cleared_value = self.value.replace('-', '')
            return
        else:
            Exception('Id type must be in {}'.format(IdType.ALL))

    @staticmethod
    def get_frequency(value):
        c = collections.Counter(value)
        frequencies = list(c.values())
        var = 0

        if len(frequencies) > 1:
            var = IdentifierHelper.frequency_variance(frequencies, len(value))

        return var

    @staticmethod
    def get_alternating(value):
        diff = []
        for i in range(len(value) - 1):
            signum = np.sign(ord(value[i + 1]) - ord(value[i]))
            diff.append(signum)
        var = IdentifierHelper.frequency_variance(diff, len(diff))
        return float(var)

    @staticmethod
    def get_periodicity(value):
        freqs = []
        for j in range(2, len(value)):
            for i in range(0, j):
                cur_seq = value[i::j]
                if len(cur_seq) > 1:
                    c = collections.Counter(cur_seq)
                    cur_freqs = [val - 1 for val in c.values()]
                    freqs.append(sum(cur_freqs))
        return sum(freqs)

    @staticmethod
    def get_repetitions(value):
        string_counter = collections.defaultdict(int)
        for length in range(2, len(value)):
            for start in range(0, len(value) - length + 1):
                string_counter[value[start: start + length]] += 1

        repetitions = [elem - 1 for elem in string_counter.values()]

        return sum(repetitions)

    @staticmethod
    def get_neighbours(value, type, neighbours_df=None, ss=None):
        if neighbours_df is None:
            return 1

        if ss is None:
            raise Exception('Need spark client')

        neighbours_df_name = 'neighbours_table'
        neighbours_df.registerTempTable(neighbours_df_name)

        query = SQLHelper.render_query(
            SQLQuery.GET_NEIGHBOURS_NUMBER,
            neighbours=neighbours_df_name,
            id=value,
            type=type)

        neighbours_number = ss.sql(query).toPandas().neighbours_number[0]
        return int(neighbours_number)
