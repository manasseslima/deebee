import os
import aiopg
import psycopg2


def get_dsn(base_key=''):
    user, password, database, host, port = get_db_params(base_key=base_key).values()
    dsn = f"dbname={database} user={user} password={password} host={host} port={port}"
    return dsn


def get_db_params(base_key=''):
    prefix = f'DB_{base_key.upper()}' if base_key else 'DB'
    params = {
        'user': os.getenv(f'{prefix}_USER'),
        'password': os.getenv(f'{prefix}_PASSWORD'),
        'database': os.getenv(f'{prefix}_NAME'),
        'host': os.getenv(f'{prefix}_HOST'),
        'port': os.getenv(f'{prefix}_PORT', 5432),
    }
    return params


def get_connection_string():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_DATABASE')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return connection_string


def get_connection_params():
    conn_params = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }
    return conn_params
