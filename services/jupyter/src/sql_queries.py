# SELECT TABLES
songplays_table_select = '''
SELECT
    se.ts AS start_time,
    se.userid AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid AS session_id,
    se.location,
    se.useragent AS user_agent,
    EXTRACT(month FROM se.ts) as month,
    EXTRACT(year FROM se.ts) as year
FROM staging_events AS se
    LEFT JOIN staging_songs AS ss
        ON  se.song   = ss.title
        AND se.artist = ss.artist_name
        AND se.length = ss.duration
WHERE
    se.page       = 'NextSong'
    AND se.userid is NOT NULL
ORDER BY year, month
'''

users_table_select = '''
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM (
    SELECT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        LAST(level) AS level,
        LAST(ts) AS ts
    FROM staging_events
    WHERE userId IS NOT NULL
    GROUP BY user_id, first_name, last_name, gender
    ORDER BY ts DESC
)
ORDER BY user_id
'''

songs_table_select = '''
SELECT
    DISTINCT song_id AS song_id,
    title AS title,
    artist_id AS artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
SORT BY year, artist_id
'''

artists_table_select = '''
SELECT
    DISTINCT artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
SORT BY artist_id
'''

time_table_select = '''
SELECT
    DISTINCT ts as start_time,
    EXTRACT(hour FROM ts) as hour,
    EXTRACT(day FROM ts) as day,
    EXTRACT(week FROM ts) as week,
    EXTRACT(month FROM ts) as month,
    EXTRACT(year FROM ts) as year,
    EXTRACT(dayofweek FROM ts) as weekday
FROM staging_events
ORDER BY start_time
'''
