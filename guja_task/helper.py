import os
import logging
import json

import pandas as pd
import requests

from guja_task.utils import open_json
from utils import create_psql_engine
from constants import CREDS_PATH


def open_json_safe(filename):
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    return data


def convert_2_sql(data_type):
    if data_type == int:
        return 'Bigint'
    elif data_type == float:
        return ' double precision'
    elif data_type == str:
        return 'varchar'
    else:
        return "Error while casting data"


def create_table_query(values):
    types = []

    for value in values.values():
        value_type = convert_2_sql(type(value))
        types.append(value_type)

    just_columns = values.keys()

    create_data_raw = tuple(zip(just_columns, types))
    create_data_parts = [str(x[0] + " " + x[1] + ", ") for x in create_data_raw]
    create_data_str = ''.join(create_data_parts)
    ready = create_data_str.strip(', ')

    cq = f"""Create table covid_data({ready})"""

    return cq


def insert_table_query(columns):
    columns_str = ','.join(columns)
    query = f"""INSERT INTO covid_data ({columns_str}) VALUES %s"""

    return query


def select_all_query(table):
    query = f"""Select * from {table}"""

    return query


def select_by_country(table, c):
    query = f"""Select * from {table} where country='{c}'"""

    return query


def select_by_column(table, column, value):
    query = f"""Select * from {table} where {column}='{value}'"""

    return query


def parse(url, params=None):
    """
    :param url: URL of the API endpoint
    :param params: query parameters for endpoint
    :return: json object if response-ok : Error message
    """

    if not params:
        response = requests.get(url)
    else:
        response = requests.get(url, params)

    if response.ok:
        return response.json()
    else:
        logging.debug(f"Something went wrong: {response.status_code}")
        return None


def json_2_psql(file, conn, table_name, orient='records', historically=False):
    """

    :param file: filepath to be read
    :param conn: SQLAlchemy engine for connection
    :param table_name: postgres table name
    :param orient: style in which data is stored in json
    :param historically: Wheter this is historical data or not (historical data needs to be transformed specifically)
    :return: creates postgres table, returns nothing
    """

    df = pd.read_json(file, orient=orient)

    if historically:
        df['timeline'] = df['timeline'].apply(json.dumps)

    df.to_sql(table_name, conn, index=False)


def csv_2_psql(file, conn, table_name):
    """

    :param file: filepath to be read
    :param conn: SQLAlchemy engine for connection
    :param table_name: postgres table name
    :return: creates postgres table, returns nothing
    """
    df = pd.read_csv(file)
    df.to_sql(table_name, con=conn, if_exists="append", index=False)

    return df


def df_2_json(df, file_path, orient='records'):
    """
    :param df: Pandas Dataframe to be saved
    :param file_path: json file name to be created
    :param orient: used to determine rule for storing data
    :return: creates json file from given dataframe, returns nothing
    """
    df.to_json(file_path, orient=orient)


def df_2_csv(df, file_path, index=False):
    """
    :param df: Pandas Dataframe to be saved
    :param file_path: csv file name to be created
    :param index: whether index column will be created csv or not
    :return: creates csv file from given dataframe, returns TRUE if so
        """
    df.to_csv(file_path, index=index)


def connect_2_psql(creds_path):
    """
    :param creds_path: path to the credentials for connecting postgres
    :return: created engine by SQLAlchemy
    """

    creds = open_json(creds_path)
    logging.debug("Successfully got creds for postgres")

    engine = create_psql_engine(creds)
    logging.debug("Successfully created postgres engine")

    return engine


def transform_h_data(df):
    """
    :param df: Dataframe to be transformed
    :return: Transformed Dataframe
    """
    df['timeline'] = df['timeline'].apply(json.dumps)

    return df


def measure_data_quality(db_table, dw_data):
    engine = connect_2_psql(CREDS_PATH)
    result = engine.execute(f"""SELECT pg_size_pretty( pg_total_relation_size('{db_table}') );""")
    result_as_list = result.fetchall()
    db_size = result_as_list[0][0]

    result = engine.execute(f"""SELECT count(*) from {db_table}""")
    result_as_list = result.fetchall()
    db_rows = result_as_list[0][0]

    psa_size = os.path.getsize(dw_data)
    psa_rows = len(open_json_safe(dw_data))

    quality_percentage = round((psa_rows - db_rows) / psa_rows, 3)

    psa_data = f'Incoming Data: {round(int(psa_size)/1024, 2)} kB | {psa_rows} rows\n'
    dw_data = f'DW Data: {db_size} | {db_rows} rows\n'
    summary = f'Data Quality: {quality_percentage}% of data lost in ETL\n'

    all_stats = [psa_data, dw_data, summary]

    with open('ISS_Data/data_quality.txt', 'w') as f:
        f.writelines(all_stats)
       