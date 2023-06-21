"""
This code organizes and generates the SQL queries necessary to construct the data
tables for this application.

Sources include:

	ETL in Redshift exercise
	Distribution Styles exercise
	configparser documentation: https://docs.python.org/3/library/configparser.html
	ChatGPT
	Udacity GPT
	Google
	Github references:
	https://github.com/davidrubinger/udacity-dend-project-3
	https://github.com/cheuklau/udacity-data-eng-redshift
	https://github.com/jukkakansanaho/udacity-dend-project-3
	https://flynn.hashnode.dev/how-to-create-data-warehouse-with-aws-redshift

"""

import configparser  # configuration file parser

# CONFIG loads configuration data including user and database names and credentials

config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN	= config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES this will drop all tables created using this software - this is a list of queries

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES - queries to create all tables to be used for the creation
# and operation the Song Play star database
# note 1:
# 	The assumption is that all tables are deleted before creation,
# this will be enforced programmatically

# create staging_events table
staging_events_table_create = ("""
CREATE TABLE staging_events(
	event_entry INTEGER IDENTITY(0,1) PRIMARY KEY,
	artist VARCHAR,
	auth VARCHAR,
	firstName VARCHAR,
	gender CHAR,
	itemInSession INTEGER,
	lastName VARCHAR,
	length NUMERIC,
	level VARCHAR,
	location VARCHAR,
	method VARCHAR,
	page VARCHAR,
	registration NUMERIC,
	sessionId INTEGER,
	song VARCHAR,
	status INTEGER,
	ts BIGINT,
	userAgent VARCHAR,
	userId INTEGER
	);
""")


# 	artist VARCHAR NOT NULL,
# 	auth VARCHAR,
# 	firstName VARCHAR,
# 	gender CHAR,
# 	itemInSession INTEGER,
# 	lastName VARCHAR,
# 	length NUMERIC,
# 	level VARCHAR,
# 	location VARCHAR,
# 	method VARCHAR,
# 	page VARCHAR NOT NULL,
# 	registration NUMERIC,
# 	sessionId INTEGER,
# 	song VARCHAR,
# 	status INTEGER,
# 	ts INTEGER NOT NULL,
# 	userAgent VARCHAR,
# 	userId INTEGER NOT NULL



# create staging_songs table
staging_songs_table_create = ("""
CREATE TABLE staging_songs(
	song_entry INTEGER IDENTITY(0,1) PRIMARY KEY,
	num_songs INTEGER,
	artist_id VARCHAR NOT NULL,
	artist_latitude VARCHAR,
	artist_longitude VARCHAR,
	artist_location VARCHAR,
	artist_name VARCHAR NOT NULL,
	song_id VARCHAR NOT NULL,
	title VARCHAR,
	duration NUMERIC,
	year INTEGER
    );
""")

# create songplays table
songplay_table_create = ("""
	CREATE TABLE songplays(
	songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
	start_time TIMESTAMP,
	user_id VARCHAR,
	level VARCHAR,
	song_id VARCHAR NOT NULL,
	artist_id VARCHAR,
	session_id VARCHAR,
	location VARCHAR,
	user_agent VARCHAR
	);
""")

# create users table
user_table_create = ("""
CREATE TABLE users(
	user_id INTEGER PRIMARY KEY,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	level VARCHAR
	);
""")

# create songs table
song_table_create = ("""
CREATE TABLE songs(
	song_id VARCHAR PRIMARY KEY,
	title VARCHAR,
	artist_id VARCHAR,
	year INTEGER,
	duration NUMERIC
	);
""")

# create artists table
artist_table_create = ("""
CREATE TABLE artists(
	artist_id VARCHAR PRIMARY KEY,
	name VARCHAR,
	location VARCHAR,
	latitude DECIMAL,
	longitude DECIMAL
	);
""")

# create time table
time_table_create = ("""
CREATE TABLE time(
	start_time TIMESTAMP PRIMARY KEY,
	hour INTEGER,
	day INTEGER,
	week INTEGER,
	month INTEGER,
	year INTEGER,
	weekday INTEGER
	);
""")

# STAGING TABLES - raw data from the S3 repository is loaded into these tables
# for subsequent loading to analytics tables

# copy raw data from S3 repository specified in the dwh.cfg file into local staging_events table
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

# copy raw data from S3 repository specified in the dwh.cfg file into local staging_songs table
staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES - These queries will populate the analytics tables from the staging tables

# insert data from staging tables into songplays table
songplay_table_insert = ("""
	INSERT INTO songplays (
	  start_time,
	  user_id,
	  level,
	  song_id,
	  artist_id,
	  session_id,
	  location,
	  user_agent
	)
	SELECT DISTINCT
	  TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second' AS start_time,
	  staging_events.userId AS user_id,
	  staging_events.level AS level,
	  staging_songs.song_id AS song_id,
	  staging_songs.artist_id AS artist_id,
	  staging_events.sessionId AS session_id,
	  staging_events.location AS location,
	  staging_events.userAgent AS user_agent
	FROM staging_events
	JOIN staging_songs ON (staging_events.artist = staging_songs.artist_name)
	WHERE staging_events.page = 'NextSong';
""")

# insert data from staging tables into users table
user_table_insert = ("""
	INSERT INTO users (
	  user_id,
	  first_name,
	  last_name,
	  gender,
	  level
	)
	SELECT DISTINCT
	  userId AS user_id,
	  firstName AS first_name,
	  lastName AS last_name,
	  gender,
	  level
	FROM staging_events
	WHERE page = 'NextSong';
""")

# insert data from staging tables into songs table
song_table_insert = ("""
	INSERT INTO songs (
	  song_id,
	  title,
	  artist_id,
	  year,
	  duration
	)
	SELECT DISTINCT
	  song_id,
	  title,
	  artist_id,
	  year,
	  duration
	FROM staging_songs;
""")

# insert data from staging tables into artists table
artist_table_insert = ("""
	INSERT INTO artists (
	  artist_id,
	  name,
	  location,
	  latitude,
	  longitude
	)
	SELECT DISTINCT
	  artist_id,
	  artist_name AS name,
	  artist_location AS location,
	  artist_latitude AS latitude,
	  artist_longitude AS longitude
	FROM staging_songs;
""")

# insert data from staging tables into time table
time_table_insert = ("""
	INSERT INTO time (
	  start_time,
	  hour,
	  day,
	  week,
	  month,
	  year,
	  weekday
	)
	SELECT DISTINCT
	  TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
	  EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
	  EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
	  EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
	  EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
	  EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
	  EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
	FROM staging_events
	WHERE page = 'NextSong';
""")

# QUERY LISTS - each elements of the lists corresponds to a SQL query command defined above
# these are grouped by function and accessed via the create_tables.py and the etl.py programs.

# create all tables used in the database
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

# drop existing table instances
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

# copy data from S3 repository into staging tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

# transform and insert data from staging tables to analytics tables
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
