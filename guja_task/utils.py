import json
import logging
from datetime import datetime
from glob import glob

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.DEBUG)


def drop_column(response_as_json, column):
    """

    :param response_as_json: JSON file to be updated
    :param column: column name to be dropped
    :return: updated json
    """
    for item in response_as_json:
        item.pop(column, None)

    return response_as_json


def create_psql_engine(creds):
    """

    :param creds: credentials dict(needs to have user, password, ip, port and database info)
    :return: SQLAlchemy engine for connecting postgres database
    """

    # TODO: Error handling for keys
    engine = create_engine(f"postgresql://{creds['user']}:{creds['pass']}@{creds['ip']}:{creds['port']}/{creds['db']}")

    return engine


def open_json(json_file):
    """
    :param json_file: json filepath
    :return: json file data as python dict
    """

    with open(json_file) as json_f:
        json_data = json.load(json_f)

    return json_data


def timestamp_2_datetime(timestamp):
    """

    :param timestamp: timestamp(big integer) to be converted as human-readable datetime
    :return: converted timestamp as human-readable datetime
    """
    dt_object = datetime.fromtimestamp(timestamp)
    dt_time_str = dt_object.strftime("%m.%d.%Y_%H:%M")

    return dt_time_str


def dict_2_json(dict_file, directory, json_file_name):
    """

    :param dict_file: python dictionary to be saved as json
    :param directory: dictionary in which json will be saved
    :param json_file_name: output json file name
    :return:
    """
    with open(f'{directory}{json_file_name}.json', 'a') as outfile:
        json.dump(dict_file, outfile)


def validate_raw_jsons(json_files_dir):
    """

    :param json_files_dir: directory where json(s) is/are stored
    :return: nothing returned, replaces itself as valid json(s)
    """
    for f_name in glob(f'{json_files_dir}*.json'):
        with open(f_name, 'r') as outfile:
            data = outfile.read()

        enclosed_data = '[' + data + ']'
        final = enclosed_data.replace('}{', '},{')

        result = json.loads(final)

        with open(f'{f_name}', 'w') as outfile:
            json.dump(result, outfile)


def drop_duplicates(data, column):
    """

    :param data: json data to be filtered
    :param column: column name used to identify duplicates
    :return: filtered data as Pandas DF
    """
    df = pd.read_json(data, orient='records')
    rows_before = df.shape[0]
    logging.debug(f'Got {rows_before} row(s) to be saved')
    df.drop_duplicates(subset=[column], inplace=True)
    logging.debug(f'Dropped  {rows_before - df.shape[0]} row(s) after filtering')

    return df


def create_data_mart(engine):
    """

    :param engine: sqlalchemy engine for connecting postgres table
    :return: creates view from particular table
    """
    create_view_query = """CREATE VIEW iss_25544_location AS
	Select name, id, latitude, longitude, altitude, timestamp from all_iss_data"""

    engine.execute(create_view_query)
