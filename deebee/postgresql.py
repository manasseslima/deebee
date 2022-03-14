import psycopg2


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
