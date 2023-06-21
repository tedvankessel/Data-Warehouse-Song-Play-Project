"""
verify_database.py:

This program verifies the data staging and anlytics tables used in the
Data Warehouse Song Play Project. The data base schema and table structure
are described in the README.md file
Each table in the database is tested to verify the column names and that it
contains valid data.


Usage: python verify_database.py

This program should be run last.
"""
import configparser
import psycopg2

def find_most_played_song(cur, conn):
    """ find the most played song """
    
    query = ("""   
        SELECT s.title AS most_played_song_title, COUNT(*) AS play_count
        FROM songplays sp
        JOIN songs s ON sp.song_id = s.song_id
        GROUP BY sp.song_id, s.title
        ORDER BY play_count DESC
        LIMIT 1;
    """)
    
    print("\nFind the most played song:")
    cur.execute(query)
    conn.commit()
    # Fetch all rows from the result set
    rows = cur.fetchall()
    print("\n    query = ", query, "\n")
    # Iterate over the rows and print the data
    for row in rows:
        print("    ",row)
        
def find_highest_usage_time_for_songplay(cur, conn):
    
    query = ("""
        SELECT hour, COUNT(*) AS play_count
        FROM time
        JOIN songplays ON time.start_time = songplays.start_time
        GROUP BY hour
        ORDER BY play_count DESC
        LIMIT 1;
    """)
    
    print("\nFind the highest usage time for songplay:")
    cur.execute(query)
    conn.commit()
    # Fetch all rows from the result set
    rows = cur.fetchall()
    print("\n    query = ", query, "\n")
    # Iterate over the rows and print the data
    for row in rows:
        print("    ",row)
    


def test_table(cur, conn, table_name):
    """Test a specific table"""
    print(f"\nTesting table {table_name}")
    # Get the column names from the table
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    columns = cur.fetchall()
    print(f"\n    {table_name} table columns:")
    # Print the column names
    col_string = "    columns: "
    for column in columns:
        col_string = col_string + str(column[0]) + ", "
    print(col_string)

    print(f"\n    {table_name} table data rows:")
    query = f"SELECT * FROM {table_name} LIMIT 5;"
    cur.execute(query)
    conn.commit()
    # Fetch all rows from the result set
    rows = cur.fetchall()
    # print("\n    query = ", query, "\n")
    # Iterate over the rows and print the data
    for row in rows:
        print("    ",row)

def main():
    """Main program - executes the various tests of the song play database"""
    # read user credentials and data locations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # establish connection with host DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    
    # get cursor
    cur = conn.cursor()
    
    # test the staging tables
    test_table(cur, conn, "staging_events")
    test_table(cur, conn, "staging_songs")
    
    # test the analytics tables
    test_table(cur, conn, "songplays")
    test_table(cur, conn, "users")
    test_table(cur, conn, "songs")
    test_table(cur, conn, "artists")
    test_table(cur, conn, "time")
    
    # find the most played song
    find_most_played_song(cur, conn)
    
    # Find the highest usage time for songplay
    find_highest_usage_time_for_songplay(cur, conn)
    
    # close the database connection
    conn.close()

if __name__ == '__main__':
    main()
