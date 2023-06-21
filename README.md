# Data Warehouse Song Play Project

This code implements ETL pipeline for a PostgreSQL database hosted on Redshift to address
the needs of a hypothetical corporation Sparkify. 

Per the project instructions:
Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

Raw song play data is extracted from an AWS S3 repository
to staging tables in a Redshift cluster in a PostgreSQL database. SQL statements are used to transform, regroup and create analytics tables. Subsequent queries can then performed against the analytics tables.

The raw song play data resides in three datasets on the Amazon Web Service (AWS) S3 bucket storage as follows:

's3://udacity-dend/log_data'
's3://udacity-dend/log_json_path.json'
's3://udacity-dend/song_data'

These data are copied, transformed and loaded into newly created tables according to the following schema (from the project instructions):

Fact Table

    songplays - records in event data associated with song plays i.e. records with 
        page = NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

## Code elements:
    In addition to this README.md file, the main program elements of the code include:

    dwh.cfg
        configuration file with user and database credentials

    create_table.py 
        creates the fact and dimension tables for the star schema in Redshift.

    etl.py       
        loads data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.

    sql_queries.py 
        defines SQL statements which are imported into the two files above.

The code assumes:

    1. an instance of a Redshift cluster is operational 
    2. an instance of a PosgresSQL database has been created
    3. credentials for the IAM role and Redshift cluster are provided in the dwh.cfg file
        for use at runtime
        
    note: for testing a redshift cluster was brought up using the AWS console.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

1.The user must create a python environment with the following properties:

	Python version 3.7 or higher
	Installed python packages:
	
		configparser
		psycopg2
	
	Clone the repository into the environment

2.Create a Redshift cluster instance and entery credentials in dwh.cfg file (template included):

	I used a single cluster for this work to save expense.
	Include database name and IAM credentials in the dwh.cfg file.
	
	The file structure is shown below. All fields marked "xxxxx" must be filled
	in by the user and based on the particular Redshift Cluster Created
	
		[AWS]
		KEY           = 'xxxxxx'
		SECRET        = 'xxxxxx'
		
		[CLUSTER]
		HOST          = 'xxxxx'
		DB_NAME       = 'xxxxx'
		DB_USER       = 'xxxxx'
		DB_PASSWORD   = 'xxxxx'
		DB_PORT       = '5439'
		
		[IAM_ROLE]
		ARN           = xxxxx
		
		[S3]
		LOG_DATA='s3://udacity-dend/log_data'
		LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
		SONG_DATA='s3://udacity-dend/song_data/A/A'

3.Construct the database tables, load raw data to staging tables and transform to analytics tables:

	run create_tables.py 
	run etl.py
	
4.Verify that the tables are constructed and operate correctly:

	run verify_database.py


## Running the tests

The tests are run by invoking the verify_database program:

	run verify_database.py

This program verifies the data staging and anlytics tables used in the
Data Warehouse Song Play Project. The data base schema and table structure
are described above.

Each table in the database is tested to verify the column names and that it
contains valid data.

### Coding style tests

PyLint has been run against all modules. 
I have chosen to ignore PyLint errors regarding changes to names that
with the original template names. An example is:

__sql_queries.py:382:0: C0103: Constant name "insert_table_queries" doesn't conform to UPPER_CASE naming style (invalid-name)__

I have also chosen to ignore errors relating to line length in the original project template. For example:

__create_tables.py:44:0: C0301: Line too long (104/100) (line-too-long)__

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

Eclipse IDE with PyDev:
	Eclipse IDE for Java Developers
	Version: 2019-12 (4.14.0)
	Build id: 20191212-1212

## Authors

* **Theodore van Kessel** 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments and sources
	README-Template - https://gist.github.com/PurpleBooth/109311bb0361f32d87a2
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

