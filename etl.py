"""
etl.py :
This is one of two programs that implements the Data Warehouse Song Play Project.
This program implements the Extract, Transform, Load (ETL) operations:

1. Extracts raw JSON data residing in an Amazon S3 repository and loads it into staging
tables
2. Transforms and loads the data from the staging tables to the analytics tables

Usage: python etl.py

Note:
1. The data schema is described in the README.md file.
2. The companion program to this one is create_tables.py that is run first to create
the database tables
"""
import configparser
import psycopg2
#from sql_queries import copy_table_queries, insert_table_queries
import sql_queries as sq

def load_staging_tables(cur, conn):
    """ extract data from Amazon S3 repository """

    for query in sq.copy_table_queries:
        print("executing query: \n", query)
        cur.execute(query)
        conn.commit()
        print("\n", "query done")


def insert_tables(cur, conn):
    """ transform and insert data from staging tables to analytics tables"""
    for query in sq.insert_table_queries:
        print("executing query: \n", query)
        cur.execute(query)
        conn.commit()
        print("\n", "query done")


def main():
    """ carry out extract, transform, load functions"""

    # read user credentials and data locations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # establish connection with host DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    # get cursor
    cur = conn.cursor()

    # load staging tables from Amazon S3 repository
    load_staging_tables(cur, conn)

    # transform and insert data in star analytics tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
