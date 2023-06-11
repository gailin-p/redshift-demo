import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get("IAM_ROLE", "ARN")
LOG_DATA_PATH = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA_PATH = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    artist VARCHAR(200),
    auth character varying(200),
    firstName character varying(200),
    gender character varying(5),
    itemInSession int,
    lastName character varying(200),
    length double precision,
    level character varying(10),
    location character varying(200),
    method character varying(5),
    page character varying(20),
    registration double precision, 
    sessionId int,
    song VARCHAR(200) DISTKEY,
    status int,
    ts bigint,
    userAgent character varying(200),
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    num_songs int, 
    artist_id character varying(20), 
    artist_latitude double precision, 
    artist_longitude double precision, 
    artist_location character varying(200), 
    artist_name VARCHAR(200), 
    song_id character varying(20), 
    title VARCHAR(200) DISTKEY, 
    duration double precision, 
    year int 
);
""")

# Q: what should the dist key be? 
songplay_table_create = ("""
CREATE TABLE "songplay" (
    songplay_id int IDENTITY NOT NULL,
    start_time timestamp NOT NULL REFERENCES times (start_time),
    user_id int NOT NULL REFERENCES users (user_id), 
    level character varying(10) NOT NULL,
    song_id character varying(20) NOT NULL REFERENCES songs (song_id), 
    artist_id character varying(20) NOT NULL, 
    session_id int NOT NULL,
    location character varying(200),
    user_agent character varying(200)
);
""")

# Note that unlike song IDs, user ID comes from the original events data, 
# so we don't use the IDENTITY command to create it. 
user_table_create = ("""
CREATE TABLE "users" (
    user_id int NOT NULL,
    first_name character varying(200),
    last_name character varying(200),
    gender character varying(5),
    level character varying(10) NOT NULL,
    PRIMARY KEY (user_id)
)
""")
                     
# Note that artist ID comes from the original songs data, 
# so we don't use the IDENTITY command to create it. 
artist_table_create = ("""
CREATE TABLE "artists" (
    artist_id character varying(20) NOT NULL, 
    name character varying(200) NOT NULL,
    location character varying(200),
    latitude double precision, 
    longitude double precision,
    PRIMARY KEY (artist_id)
)
""")

song_table_create = ("""
CREATE TABLE "songs" (
    song_id character varying(20) NOT NULL, 
    title character varying(200),
    artist_id character varying(20) REFERENCES artists (artist_id),
    year int,
    duration double precision,
    PRIMARY KEY (song_id)
)
""")

time_table_create = ("""
CREATE TABLE "times" (
    start_time timestamp NOT NULL,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int,
    PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = """
    copy staging_events from {}
    iam_role {}
    json {} region 'us-west-2';
""".format(LOG_DATA_PATH, IAM_ROLE, LOG_JSON_PATH) 

staging_songs_copy = """
    copy staging_songs from {}
    iam_role {}
    json 'auto' region 'us-west-2';
""".format(SONG_DATA_PATH, IAM_ROLE) 

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO "songplay" (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + ts * interval '1 second', userId, level, staging_songs.song_id, staging_songs.artist_id, sessionId, location, userAgent
FROM staging_events JOIN staging_songs ON staging_events.artist = staging_songs.artist_name AND staging_events.song = staging_songs.title
WHERE staging_events.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO "users" (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE staging_events.page='NextSong';
""")

song_table_insert = ("""
INSERT INTO "songs" (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE title IN
(
SELECT title FROM staging_events
)
""")

artist_table_insert = ("""
INSERT INTO "artists" (artist_id, name, location, latitude, longitude)
SELECT DISTINCT staging_songs.artist_id, staging_songs.artist_name, staging_songs.artist_location, staging_songs.artist_latitude, staging_songs.artist_longitude
FROM songplay JOIN staging_songs ON songplay.artist_id = staging_songs.artist_id
""")

time_table_insert = ("""
INSERT INTO "times" (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time, DATEPART(HOUR, start_time), DATEPART(DAY, start_time), DATEPART(WEEK, start_time), DATEPART(MONTH, start_time), DATEPART(YEAR, start_time), DATEPART(WEEKDAY, start_time)
FROM songplay 
""")
                     
# EXPLORATORY QUERIES 

most_played_artist = ("""
SELECT artists.name, COUNT(songplay.artist_id) AS artist_listens 
FROM songplay JOIN artists on songplay.artist_id = artists.artist_id
GROUP BY artists.name
ORDER BY artist_listens DESC
LIMIT 5;
""")
                      
most_common_location = ("""
SELECT location,
COUNT(songplay.location) AS usr_loc 
FROM songplay 
GROUP BY location
ORDER BY usr_loc DESC
LIMIT 5;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
exploratory_queries = [most_played_artist, most_common_location]
