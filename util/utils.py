import os
import re


class IdentifierHelper(object):
    device_id_pattern = re.compile('[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}')
    email_pattern = re.compile('[^@]+@[^@]+\.[^@]+')

    @staticmethod
    def frequency_variance(values, amount):
        expected = 0
        square_expected = 0

        for val in values:
            expected += val * (1.0 * val / amount)
            square_expected += val * val * (1.0 * val / amount)

        variance = square_expected - expected * expected
        return variance


class SQLHelper(object):
    @staticmethod
    def render_query(query_path, **kwargs):
        print(os.getcwd())
        with open(query_path, 'r') as f:
            query = str(f.read())
        f.close()
        return query.format(**kwargs)
