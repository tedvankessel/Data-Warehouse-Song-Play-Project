'''
ViewSTLErrorTable.py

This program allows the user to see the stl error table in AWS Redshift and has been usefull in debugging queries
to the Redshift platform

usage: python ViewSTLErrorTable.py
'''
import configparser
import psycopg2

def main():

    # read user credentials and data locations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # establish connection with host DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    # get cursor
    cur = conn.cursor()
    #query = "SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;"
    #query = "SELECT * FROM stl_load_errors WHERE table_name = 'staging_events';"
    #query = "SELECT errcode FROM stl_load_errors WHERE userid = 100 AND slice = 1 AND tbl = 119201 AND starttime = '2023-06-14 18:51:27.320996' AND query = 1073881505;"
    query = "SELECT err_reason FROM stl_load_errors WHERE userid = 100 ;"
    cur.execute(query)
    results = cur.fetchall()
    print(results)
    
    conn.close()


if __name__ == '__main__':
    main()