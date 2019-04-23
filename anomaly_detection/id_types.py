class IdType(object):
    EMAIL = 'email'
    LOGIN = 'login'
    IDFA = 'idfa'
    GAID = 'gaid'

    ALL = {EMAIL, LOGIN, IDFA, GAID}

    DEVICE_ID = {IDFA, GAID}
    HUMAN_ID = {EMAIL, LOGIN}
