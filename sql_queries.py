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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                    artist         VARCHAR,
                                    auth           TEXT      NOT NULL,
                                    firstName      VARCHAR   NOT NULL,
                                    gender         TEXT,
                                    itemInSession  INT       NOT NULL,
                                    lastName       VARCHAR,
                                    length         INT,
                                    level          TEXT      NOT NULL,
                                    location       VARCHAR,
                                    method         TEXT      NOT NULL,
                                    page           TEXT      NOT NULL,
                                    registration   INT,
                                    sessionId      INT       NOT NULL,
                                    song           VARCHAR,
                                    status         INT       NOT NULL,
                                    ts             NUMERIC   NOT NULL,
                                    userAgent      VARCHAR,
                                    userId         INT       NOT NULL
);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 num_songs           INT       NOT NULL,
                                 artist_id           VARCHAR   NOT NULL,
                                 artist_latitude     NUMERIC,
                                 artist_longitude    NUMERIC,
                                 artist_location     VARCHAR,
                                 artist_name         VARCHAR   NOT NULL,
                                 song_id             VARCHAR   NOT NULL,
                                 title               VARCHAR   NOT NULL,
                                 duration            FLOAT     NOT NULL,
                                 year                INT


);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id      INT IDENTITY(0,1)  NOT NULL           PRIMARY KEY,
                            start_time       TIMESTAMP      NOT NULL      sortkey  distkey,
                            user_id          INT            NOT NULL,
                            level            VARCHAR        NOT NULL,
                            song_id          VARCHAR,
                            artist_id        VARCHAR,
                            session_id       INT            NOT NULL,
                            location         VARCHAR        NOT NULL,
                            user_agent       VARCHAR        NOT NULL


);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id      INT       PRIMARY KEY ,
                            first_name   VARCHAR   NOT NULL,
                            last_name    VARCHAR   NOT NULL,
                            gender       VARCHAR   NOT NULL,
                            level        VARCHAR   NOT NULL
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id      VARCHAR    PRIMARY KEY,
                            title        VARCHAR    NOT NULL,
                            artist_id    VARCHAR    NOT NULL ,
                            year         INT        NOT NULL,
                            duration     NUMERIC    NOT NULL
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id    VARCHAR    PRIMARY KEY,
                            name         VARCHAR,
                            location     VARCHAR,
                            latitude     FLOAT,
                            longitude    FLOAT
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time   TIMESTAMP  NOT NULL   sortkey  distkey  PRIMARY KEY,
                            hour         INT        NOT NULL,
                            day          INT        NOT NULL,
                            week         INT        NOT NULL,
                            month        INT        NOT NULL,
                            year         INT        NOT NULL,
                            weekday      INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    iam_role {}
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON {}
    timeformat as 'epochmillisecs'
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
    iam_role {}
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])
# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + (se.ts/1000)*INTERVAL '1 second' as start_time,
        se.userId as user_id,
        se.level as level,
        se.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as  location,
        se.userAgent as user_agent
    FROM staging_events se,
    JOIN staging_songs ss
    ON (ss.title=se.song AND se.artist=ss.artist_name)
    AND se.page=' NextSong';
""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page='NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,title,artist_id,year,duration 
    FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000)*INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        TO_CHAR(start_time,'day') as weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
