import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

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
  CREATE TABLE IF NOT EXISTS staging_events(
       artist TEXT,
       auth TEXT,
       firstname TEXT,
       gender TEXT,
       ItemInSession INT,
       lastName TEXT,
       length FLOAT,
       level TEXT,
       location TEXT,
       method TEXT,
       page TEXT,
       registration TEXT,
       sessionId INT,
       song TEXT,
       status INT,
       ts BIGINT,
       userAgent TEXT,
       userId INT
)
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs(
       num_songs INT,
       artist_id TEXT,
       artist_latitude FLOAT,
       artist_longitude FLOAT,
       artist_location VARCHAR,
       artist_name TEXT,
       song_id TEXT,
       title TEXT,
       duration FLOAT,
       year INT, 
)     
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays(
       songplay_id bigint IDENTITY(0, 1) PRIMARY KEY,
       start_time TIMESTAMP,
       user_id TEXT,
       level TEXT,
       song_id TEXT,
       artist_id TEXT,
       session_id INT,
       location TEXT,
       user_agent TEXT
  )
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users(
      user_id TEXT PRIMARY KEY,
      first_name TEXT,
      last_name TEXT,
      gender TEXT,
      level TEXT
  )
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs(
       song_id TEXT PRIMARY KEY,
       title VARCHAR,
       artist_id TEXT,
       year INT,
       duration FLOAT 
  )
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artist(
       artist_id TEXT PRIMARY KEY,
       name TEXT,
       location TEXT,
       latitude FLOAT,
       longitude FLOAT
  )
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time(
       start_time TIMESTAMP PRIMARY KEY,
       hour INT ,
       day  INT,
       week INT,
       month INT,
       year INT,
       weekday INT
  )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE '{}'
    format as json ''s3://udacity-dend/log_json_path.json'
    region 'us-east-1';
""").format(
    config.get("S3","LOG_DATA"), 
    ARN,
    config.get("S3", "LOG_DATA")
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
""").format(
    config.get("S3", "SONG_DATA"), ARN
)
# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
   SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second' as
        start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM stage_event e
    JOIN staging_song s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        e.length = s.duration
    WHERE
        e.page = 'NextSong'
""")

user_table_insert = ("""
  INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT DISTINCT(e.userId), e.firstName, e.lastName, e.gender, e.level
  FROM staging_events e
  WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
  INSERT INTO songs (song_id, title, artist_id, year, duration)
  SELECT DISTINCT(s.song_id), s.title, s.artist_id, s.year, s.duration
  FROM staging_songs s
  
""")

artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, latitude, longitude)
  SELECT DISTINCT(s.artist_id), s.artist_name, s.artist_location,      s.artist_latitude, s.artist_longitude
  FROM staging_songs s
""")

time_table_insert = ("""
  INSERT INTO time(start_time, hour, day, week, month, year, weekday)
  SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts),   EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
  FROM(
  SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
  FROM staging_events WHERE page='NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
