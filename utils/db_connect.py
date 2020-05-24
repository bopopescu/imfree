from utils.cfg_parse import config
from sqlalchemy import create_engine
import psycopg2
import pyodbc


def connect(database):
    if database.lower() == 'postgre':
        pg_params = config('postgre')
        pg_conn = psycopg2.connect(dbname=pg_params['database'],
                                   host=pg_params['host'],
                                   user=pg_params['user'],
                                   password=pg_params['password'])
        return pg_conn

    elif database.lower() == 'sqlserver':
        ms_params = config('sqlserver')
        ms_conn = pyodbc.connect('Driver={SQL Server}'
                                 ';Server=%s' % ms_params['host'] +
                                 ';Database=%s' % ms_params['database'] +
                                 ';UID=%s' % ms_params['user'] +
                                 ';PWD=%s' % ms_params['password'] +
                                 ';Trusted_Connection=no'
                                 )
        return ms_conn

    elif database.lower() == 'redshift':
        rs_params = config('redshift')
        rs_conn = psycopg2.connect(dbname=rs_params['database'],
                                   host=rs_params['host'],
                                   port=rs_params['port'],
                                   user=rs_params['user'],
                                   password=rs_params['password'])
        return rs_conn

def set_engine(database):
    param = config(database)
    conn_str = "postgresql://{0}:{1}@{2}:{3}/{4}" \
        .format(param['user'], param['password'], param['host'], param['port'],
                param['database'])
    engine = create_engine(conn_str)

    return engine
