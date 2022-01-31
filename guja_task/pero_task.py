import logging

# TODO: Function names must be intuitive
# TODO: File Hierarchy and imports
# TODO: No error handling whatsoever. NO. Not a single Exception

from full_covid_data import main_full
from history_covid_data import main_historical
from constants import CREDS_PATH
from helper import df_2_json, df_2_csv, json_2_psql, csv_2_psql, connect_2_psql, transform_h_data

logging.basicConfig(level=logging.DEBUG)

# TODO: Which mode is what
MODE = 2
SAVE_JSON = 1
SAVE_CSV = 1

PSQL_RECOVERY_JSON = 1
PSQL_RECOVERY_CSV = 1


def main():
    engine = connect_2_psql(CREDS_PATH)

    if MODE == 1:
        df = main_full()

        if SAVE_JSON == 1:
            df_2_json(df, 'covid_data/covid_data.json')
            logging.debug("Json file created successfully")

        if SAVE_CSV == 1:
            df_2_csv(df, 'covid_data/covid_data.csv')
            logging.debug("CSV file created successfully")

        df.to_sql(name='covid_data', con=engine, index=False)
        logging.debug("Successfully created POSTGRESQL table from parsed data")

        if PSQL_RECOVERY_JSON == 1:
            json_2_psql('covid_data/covid_data.json', engine, 'covid_data_json', orient='index')
            logging.debug("Successfully created POSTGRESQL table from json file")

        if PSQL_RECOVERY_CSV == 1:
            csv_2_psql('covid_data/covid_data.csv', engine, 'covid_data_csv')
            logging.debug("Successfully created POSTGRESQL table from csv file")

    if MODE == 2:
        df = main_historical()

        if SAVE_JSON == 1:
            df_2_json(df, 'covid_data/covid_data_h.json')
        if SAVE_CSV == 1:
            df_2_csv(df, 'covid_data/covid_data_h.csv')

        df = transform_h_data(df)

        df.to_sql('covid_data_h', con=engine, if_exists="append", index=False)
        logging.debug("Successfully created POSTGRESQL table from parsed data")

        if PSQL_RECOVERY_JSON == 1:
            json_2_psql('covid_data/covid_data_h.json', engine, 'covid_data_h_json')
            logging.debug("Successfully created POSTGRESQL table from json file")

        if PSQL_RECOVERY_CSV == 1:
            csv_2_psql('covid_data/covid_data_h.csv', engine, 'covid_data_h_csv')
            logging.debug("Successfully created POSTGRESQL table from csv file")


if __name__ == '__main__':
    main()
