from pyspark.sql import SparkSession

import pyspark.sql.functions as F
import fire

from datetime import datetime

import configparser
import os

import sql_queries as Q


SONGPLAYS_TABLE_PARQUET = 'songplays_table.parquet'
ARTISTS_TABLE_PARQUET = 'artists_table.parquet'
SONGS_TABLE_PARQUET = 'songs_table.parquet'
USERS_TABLE_PARQUET = 'users_table.parquet'
TIME_TABLE_PARQUET = 'time_table.parquet'

SONG_DATASET_PATH = 'song_data/*/*/*/'
LOG_DATASET_PATH = 'log_data/*/*/'


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')


def as_null(value, colname):
    '''
    Column expression that changes "value" values that appear in the "colname"
    column of the dataframe into NULLs.

    Args:
        value: The value to replace with NULLs.
        colname: The name of the column where to apply the transformation.

    Returns:
        A DataFrame Column.
    '''

    return F.when(F.col(colname) != value, F.col(colname)).otherwise(None)


def create_spark_session():
    '''
    Create a Spark session configured with Apache Hadoop Amazon Web Services
    support.

    Returns:
        A Spark session.
    '''

    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Transform raw JSON song data into analytics tables in Parquet format.

    Args:
        spark: A Spark session
        input_data: A directory to get the JSON data from.
        output_data: A directory to write the result Parquet files.
    '''

    # read song data files
    song_data = os.path.join(input_data, SONG_DATASET_PATH)
    df = spark.read.json(song_data, multiLine=True)

    # transform empty strings and zeros into NULLs
    df = df.withColumn('artist_location', as_null('', 'artist_location'))
    df = df.withColumn('song_id', as_null('', 'song_id'))
    df = df.withColumn('year', as_null(0, 'year'))

    # create a staging table for song data
    df.createOrReplaceTempView('staging_songs')

    # extract columns to create songs table
    songs_table = spark.sql(Q.songs_table_select)

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = os.path.join(output_data, SONGS_TABLE_PARQUET)
    songs_table.write.parquet(songs_table_path, mode='overwrite',
        partitionBy=('year', 'artist_id'))

    # extract columns to create artists table
    artists_table = spark.sql(Q.artists_table_select)

    # write artists table to parquet files
    artists_table_path = os.path.join(output_data, ARTISTS_TABLE_PARQUET)
    artists_table.write.parquet(artists_table_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Transform raw JSON log data into analytics tables in Parquet format.

    Args:
        spark: A Spark session
        input_data: A directory to get the JSON data from.
        output_data: A directory to write the result Parquet files.
    '''

    # read song data file
    log_data = os.path.join(input_data, LOG_DATASET_PATH)
    df = spark.read.json(log_data)

    # transform empty strings into NULLs, and make userId be an integer value
    df = df.withColumn('userId', as_null('', 'userId'))
    df = df.withColumn('userId', F.expr('cast(userId as int)'))

    # transform ts column into timestamp
    df = df.withColumn('ts', F.from_unixtime(df.ts / 1000))

    # create a staging table for log data
    df.createOrReplaceTempView('staging_events')

    # extract columns for users table
    users_table = spark.sql(Q.users_table_select)

    # write users table to parquet files
    users_table_path = os.path.join(output_data, USERS_TABLE_PARQUET)
    users_table.write.parquet(users_table_path, mode='overwrite')

    # extract columns to create time table
    time_table = spark.sql(Q.time_table_select)

    # write time table to parquet files partitioned by year and month
    time_table_path = os.path.join(output_data, TIME_TABLE_PARQUET)
    time_table.write.parquet(time_table_path, mode='overwrite',
        partitionBy=('year', 'month'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(Q.songplays_table_select)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = os.path.join(output_data, SONGPLAYS_TABLE_PARQUET)
    songplays_table.write.parquet(songplays_table_path, mode='overwrite',
        partitionBy=('year', 'month'))


def main(input_data='s3a://udacity-dend/', output_data=''):
    '''
    Run the ETL pipeline.

    Extract all files from song and log directories in "input_data", load the
    schema-on-read tables, perform data transformations and load them back into
    "output_data" as Parquet files.

    Args:
        input_data: A directory to get the JSON data from. Can be a local path
        or an S3 bucket.
        output_data: A directory to write the result Parquet files. Can be a
        local path or an S3 bucket.
    '''

    spark = create_spark_session()

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == '__main__':
    fire.Fire(main)
