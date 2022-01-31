import logging

import pandas as pd

from helper import parse
from utils import drop_column
# TODO: No *
from constants import *

logging.basicConfig(level=logging.DEBUG)


def main_full():
    res = parse(URL)

    if res:
        logging.debug("Data parsed successfully")

        res = drop_column(res, 'countryInfo')
        df = pd.DataFrame(res)
        logging.debug("Successfully created df from parsed data")

        return df

    else:
        # TODO: I have no idea what is the result
        logging.debug(f"{res}")
