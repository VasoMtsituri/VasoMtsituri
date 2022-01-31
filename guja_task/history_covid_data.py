import logging

import pandas as pd

from helper import parse
from constants import *

logging.basicConfig(level=logging.DEBUG)


def main_historical():
    url_built = f'{URL_H}{COUNTRY}?lastdays={LAST_DAYS}'

    res = parse(url_built)

    if res:
        logging.debug("Data parsed successfully")

        df = pd.DataFrame.from_dict(res, orient='index')
        df = df.T

        return df

    else:
        logging.debug(f"{res}")
