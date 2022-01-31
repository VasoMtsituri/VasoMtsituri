import logging
import time

import schedule

from helper import parse, connect_2_psql, measure_data_quality
from constants import ISS_URL, CREDS_PATH
from utils import timestamp_2_datetime, dict_2_json, validate_raw_jsons, drop_duplicates, create_data_mart

logging.basicConfig(level=logging.DEBUG)

previous_timestamp = 0


def parse_job():
    global previous_timestamp
    start = time.perf_counter()

    response = parse(ISS_URL)
    if response:
        timestamp_from_res = response['timestamp']
        if timestamp_from_res != previous_timestamp:
            logging.debug(f'Got res with timestamp: {timestamp_from_res}')
            time_stamp = timestamp_2_datetime(timestamp_from_res)
            dict_2_json(response, 'ISS_Data/all/', 'all_data')
            dict_2_json(response, 'ISS_Data/by_timestamp/', time_stamp)
            end = time.perf_counter()
            logging.debug(f'Requested the tutorial in {end - start:0.4f} seconds')


def main():
    schedule.every(0).seconds.do(parse_job)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
    validate_raw_jsons('ISS_Data/all/')
    validate_raw_jsons('ISS_Data/by_timestamp/')
    df = drop_duplicates('ISS_Data/all/all_data.json', 'timestamp')

    engine = connect_2_psql(CREDS_PATH)

    df.to_sql('all_iss_data', con=engine, if_exists="append", index=False)
    logging.debug(f'Successfully appended {df.shape[0]} row(s) to the SQL Table')

    measure_data_quality('all_iss_data', 'ISS_Data/all/all_data.json')

    create_data_mart(engine)
