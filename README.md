###Purpose of the project 
In a given scenario by Udacity.com, Sparkify(a music streaming startup) needs to move their processes and data onto the cloud. This is done by creating an ETL pipeline to load data from S3 to tables on Redshift. Two datasets in S3 are given which are the song data and the log data. 

First, a star schema is created. The songplays table is the fact table and the users, songs, artists, and time tables are the dimension tables. The appropriate SQL statements are in sql_queries.py. 
The create_tables.py script will create the tables on the star schema. A redshift cluster is launched and an IAM role that has read access to S3 is created. Both are added to the dwh.cfg file. Finally, the etl.py script will load the data from S3 to Redshift.

###Database Schema
* Fact Table: songplay_table
  * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Tables
  * user_table
    * user_id, first_name, last_name, gender, level
  * song_table
    * song_id, title, artist_id, year, duration
  * artist_table
    * artist_id, name, location, latitude, longitude
  * time_table
    * start_time, hour, day, week, month, year, weekday


###How to Run
1. Launch a Redshift cluster and create an IAM role that has read access to S3.
2. Fill in the appropriate information in the dwh.cfg file.
3. Run create_tables.py
4. Run etl.py