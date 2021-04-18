import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop =  "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table (
                    artist varchar, 
                    auth varchar NOT NULL,
                    firstName varchar NOT NULL, 
                    gender varchar NOT NULL, 
                    itemInSession int NOT NULL, 
                    lastName varchar NOT NULL, 
                    length decimal NOT NULL, 
                    level varchar NOT NULL,
                    location varchar NOT NULL,
                    method varchar NOT NULL,
                    page varchar NOT NULL, 
                    registration decimal NOT NULL,
                    sessionId int NOT NULL, 
                    song varchar NOT NULL, 
                    status int NOT NULL,
                    ts timestamp NOT NULL, 
                    userAgent varchar NOT NULL,
                    userId int NOT NULL);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table (
                    num_songs int NOT NULL, 
                    artist_id varchar NOT NULL, 
                    artist_latitude varchar NOT NULL,
                    artist_longitude varchar NOT NULL, 
                    artist_location varchar NOT NULL, 
                    artist_name varchar NOT NULL,
                    song_id varchar NOT NULL, 
                    title varchar NOT NULL, 
                    duration numeric NOT NULL, 
                    year int NOT NULL);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
                songplay_id int identity(0,1) PRIMARY KEY,
                start_time varchar NOT NULL,
                user_id int NOT NULL,
                level varchar NOT NULL, 
                song_id varchar,
                artist_id varchar, 
                session_id int NOT NULL,
                location varchar NOT NULL,
                user_agent varchar NOT NULL);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
          user_id int PRIMARY KEY, 
          first_name varchar NOT NULL, 
          last_name varchar NOT NULL,
          gender varchar NOT NULL, 
          level varchar NOT NULL);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
            song_id varchar PRIMARY KEY, 
            title varchar NOT NULL,
            artist_id varchar, 
            year int NOT NULL, 
            duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
            artist_id varchar PRIMARY KEY,
            name varchar NOT NULL, 
            location varchar NOT NULL, 
            latitude numeric, 
            longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
            start_time timestamp PRIMARY KEY, 
            hour int NOT NULL,
            day varchar NOT NULL, 
            week int NOT NULL, 
            month varchar NOT NULL,
            year int NOT NULL, 
            weekday varchar NOT NULL);""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events_table FROM {} iam_role '{}' FORMAT AS JSON {} TIMEFORMAT AS 'epochmillisecs';""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs_table FROM {} iam_role '{}' FORMAT AS JSON 'auto';""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (
                start_time, user_id, 
                level, 
                song_id, 
                artist_id, 
                session_id, 
                location, 
                user_agent) 
                SELECT DISTINCT events.ts AS start_time,
                events.userId AS user_id, 
                events.level AS level,
                songs.song_id AS song_id, 
                songs.artist_id AS artist_id, 
                events.sessionId AS session_id,
                events.location AS location,
                events.userAgent AS user_agent
                FROM staging_events_table events 
                JOIN staging_songs_table songs ON (events.song = songs.title AND events.artist = songs.artist_name)
                AND events.page == 'NextSong';""")

user_table_insert = ("""INSERT INTO user_table (
            user_id,
            first_name, 
            last_name,
            gender, 
            level) 
            SELECT DISTINCT userId AS user_id,
            firstName AS first_name, 
            lastName AS last_name, 
            gender,level 
            FROM staging_events_table
            WHERE user_id IS NOT NULL
            AND page == 'NextSong'; """)

song_table_insert = ("""INSERT INTO song_table (
            song_id,
            title, 
            artist_id,
            year, 
            duration)
            SELECT DISTINCT song_id,
            title,
            artist_id,
            year,
            duration 
            FROM staging_songs_table 
            WHERE song_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO artist_table (
            artist_id,
            name, 
            location,
            latitude, 
            longitude)
            SELECT DISTINCT artist_id,
            artist_name AS name, 
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude 
            FROM staging_songs_table 
            WHERE artist_id IS NOT NULL;""")


time_table_insert = ("""INSERT INTO time (
            start_time,
            hour, 
            day,
            week,
            month,
            year,
            weekday)
            SELECT DISTINCT start_time, 
            EXTRACT(hour FROM start_time) AS hour, 
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday 
            FROM songplay_table;""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
