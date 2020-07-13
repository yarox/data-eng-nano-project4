from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from datetime import datetime

import configparser
import os


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']


def as_null(value, colname):
    return F.when(F.col(colname) != value, F.col(colname)).otherwise(None)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # read song data file
    df = spark.read.json(input_data, multiLine=True)

    # transform empty strings and zeros into NULLs
    df = df.withColumn('artist_location', as_null('', 'artist_location'))
    df = df.withColumn('song_id', as_null('', 'song_id'))
    df = df.withColumn('year', as_null(0, 'year'))

    # create a staging table for song data
    df.createOrReplaceTempView('staging_songs')

    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT
            DISTINCT song_id AS song_id,
            title AS title,
            artist_id AS artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
        SORT BY year, artist_id
    ''')

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = os.path.join(output_data, 'songs_table.parquet')
    songs_table.write.parquet(songs_table_path, mode='overwrite',
        partitionBy=('year', 'artist_id'))

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT
            DISTINCT artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
        SORT BY artist_id
    ''')

    # write artists table to parquet files
    artists_table_path = os.path.join(output_data, 'artists_table.parquet')
    artists_table.write.parquet(artists_table_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # read song data file
    df = spark.read.json(input_data, multiLine=True)

    # transform empty strings into NULLs, and make userId be an integer value
    df = df.withColumn('userId', as_null('', 'userId'))
    df = df.withColumn('userId', F.expr('cast(userId as int)'))

    # transform ts column into timestamp
    df = df.withColumn('ts', F.from_unixtime(log_df.ts / 1000))

    # create a staging table for log data
    df.createOrReplaceTempView('staging_events')

    # extract columns for users table
    users_table = spark.sql('''
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
    ''')

    # write users table to parquet files
    users_table_path = os.path.join(output_data, 'users_table.parquet')
    users_table.write.parquet(users_table_path, mode='overwrite')

    # extract columns to create time table
    time_table = spark.sql('''
        SELECT
            DISTINCT ts,
            EXTRACT(hour FROM ts) as hour,
            EXTRACT(day FROM ts) as day,
            EXTRACT(week FROM ts) as week,
            EXTRACT(month FROM ts) as month,
            EXTRACT(year FROM ts) as year,
            EXTRACT(dayofweek FROM ts) as weekday
        FROM staging_events
        ORDER BY ts
    ''')

    # write time table to parquet files partitioned by year and month
    time_table_path = os.path.join(output_data, 'time_table.parquet')
    time_table.write.parquet(time_table_path, mode='overwrite',
        partitionBy=('year', 'month'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''
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
            EXTRACT(year FROM se.ts) as year,
        FROM staging_events AS se
            LEFT JOIN staging_songs AS ss
                ON  se.song   = ss.title
                AND se.artist = ss.artist_name
                AND se.length = ss.duration
        WHERE
            se.page       = 'NextSong'
            AND se.userid is NOT NULL
        ORDER BY year, month
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = os.path.join(output_data, 'songplays_table.parquet')
    songplays_table.write.parquet(songplays_table_path, mode='overwrite',
        partitionBy=('year', 'month'))


def main():
    spark = create_spark_session()

    output_data = ''
    input_data = 's3a://udacity-dend/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
