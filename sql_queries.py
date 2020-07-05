import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length float8,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId varchar NOT NULL,
    song varchar,
    status varchar,
    ts bigint NOT NULL,
    userAgent varchar,
    userId varchar NOT NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar NOT NULL,
    artist_name varchar,
    artist_location varchar,
    artist_latitude float8,
    artist_longitude float8,
    song_id varchar NOT NULL,
    title varchar,
    year int4,
    duration float8
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int4,
    duration float8
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude float8,
    longitude float8
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES
staging_events_copy = ("""COPY staging_events from {}
                        CREDENTIALS 'aws_iam_role={}'
                        region 'us-west-2'
                        COMPUPDATE OFF STATUPDATE OFF
                        JSON {} """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs from {}
                        CREDENTIALS 'aws_iam_role={}'
                        region 'us-west-2'
                        COMPUPDATE OFF STATUPDATE OFF 
                        JSON 'auto' """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    e.location,
    e.useragent
FROM staging_events e
LEFT JOIN staging_songs s
    ON (e.song = s.title)
    AND (e.artist = s.artist_name)
    AND (e.length = s.duration)
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
    SELECT 
    DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT 
    DISTINCT start_time,
    EXTRACT (HOUR FROM start_time),
    EXTRACT (DAY FROM start_time),
    EXTRACT (WEEK FROM start_time),
    EXTRACT (MONTH FROM start_time),
    EXTRACT (YEAR FROM start_time), 
    EXTRACT (WEEKDAY FROM start_time)
FROM songplays
""")

# RESULT Queries
staging_events = ("""SELECT count(*) FROM staging_events""")
staging_songs = ("""SELECT count(*) FROM staging_songs""")
songplays = ("""SELECT count(*) FROM songplays""")
users = ("""SELECT count(*) FROM users""")
songs = ("""SELECT count(*) FROM songs""")
artists = ("""SELECT count(*) FROM artists""")
time = ("""SELECT count(*) FROM time""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
result_queries = [staging_events, staging_songs, users, songs, artists, time]

# Print statements keys and values
copy = {staging_events_copy : 'staging_events_copy', staging_songs_copy : 'staging_songs_copy'}

create = {staging_events_table_create : 'staging_events_table_create', staging_songs_table_create : 'staging_songs_table_create', songplay_table_create : 'songplay_table_create', user_table_create : 'user_table_create', song_table_create : 'song_table_create', artist_table_create : 'artist_table_create', time_table_create : 'time_table_create'}

insert = {songplay_table_insert : 'songplay_table_insert', user_table_insert : 'user_table_insert', song_table_insert : 'song_table_insert', artist_table_insert : 'artist_table_insert', time_table_insert : 'time_table_insert'}

result = {staging_events : 'staging_events', staging_songs : 'staging_songs ', users : 'users         ', songs : 'songs         ', artists : 'artists       ', time : 'time          '}