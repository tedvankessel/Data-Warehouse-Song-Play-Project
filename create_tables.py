"""
create_tables.py:
This program creates the data staging and anlytics tables used in the
Data Warehouse Song Play Project. The data base schema and table structure
are described in the README.md file

Usage: python create_tables.py

This program should be run first.
"""
import configparser
import psycopg2
# from sql_queries import create_table_queries, drop_table_queries
import sql_queries as sq



def drop_tables(cur, conn):
    """ drop any previous instances of the tables """
    for query in sq.drop_table_queries:
        print("executing query: \n", query)
        cur.execute(query)
        conn.commit()
        print("\n", "query done")


def create_tables(cur, conn):
    """ run the database scripts in the sql_queries.py file to create the tables """
    for query in sq.create_table_queries:
        print("executing query: \n", query)
        cur.execute(query)
        conn.commit()
        print("\n", "query done")


def main():
    """ carry out the create tables functions"""

    config = configparser.ConfigParser()    # Read user and database credentials
    config.read('dwh.cfg')


    # establish connection
    connect_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    print(connect_string)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)      # drop any remaining tables from previous use
    create_tables(cur, conn)    # create new tables (empty)

    conn.close()


if __name__ == "__main__":
    main()
