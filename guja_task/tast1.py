import psycopg2
from psycopg2.extras import execute_values
from helper import *
import logging


URL = 'https://disease.sh/v3/covid-19/countries'


# This is the helper function

# Must be in UTILS
def prepare_data(response):
    for item in response:
        item.pop('countryInfo', None)

    return response


# Must be in UTILS
def conn_2_psql():
    conn = psycopg2.connect(dbname='postgres', user='postgres', host='localhost', password='12345')

    return conn


def export2psql(con, data):
    cur = con.cursor()
    logging.info('Successfully logged in PostgreSQL')

    create_query = create_table_query(data[0])
    cur.execute(create_query)

    logging.info('Table created successfully')

    values = [[value for value in project.values()] for project in data]

    # Assuming that all the json objects have similar columns ?!
    insert_query = insert_table_query(data[0].keys())
    execute_values(cur, insert_query, values)

    logging.info('Data inserted successfully')
    con.commit()

    # ######################Selects##########################################

    # Select all
    print("##################################################################################")
    print("Select all:")
    cur.execute(select_all_query('covid_data'))
    records = cur.fetchall()
    print(records)

    # Select by country
    print("##################################################################################")
    print("Select by country:")
    cur.execute(select_by_country('covid_data', 'Georgia'))
    records = cur.fetchall()
    print(records)

    # Select by column
    print("##################################################################################")
    print("Select by column:")

    cur.execute(select_by_column('covid_data', 'todaycases', '0'))
    records = cur.fetchall()
    print(f" Today cases with 0: {len(records)} country(es)")

    cur.close()
    con.close()


def main():

    """
    [1] Test connection
    [2] Ingest Data
    [3] Data Quality [E[T]L]
    [4] Insert

    """

    res = parse(URL)
    data = prepare_data(res)

    con = conn_2_psql()
    export2psql(con, data)


if __name__ == '__main__':
    main()
    print('Success')
