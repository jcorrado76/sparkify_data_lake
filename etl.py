"""The list of columns that I need in my songplays fact table are:
    * songplay_id - generated with monotonically increasing id
    * start_time - from the log table
    * userId - from the log table
    * level - from the log table
    * song_id - from the songs table
    * artist_id - from the songs *and* artists table - you must drop the one
    from the songs table
    * sessionId - from the logs table
    * location - the one from the logs table, not the artists.artist_location
    * userAgent - from the logs table
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,\
from pyspark.sql.functions import col, year, month, dayofmonth, hour, weekofyear, dayofweek, from_unixtime, to_timestamp

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """This function creates a spark session that can interact via AWS S3
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This function loads the song metadata files into the spark cluster, performs
    whatever processing is necessary, then splits and saves the resulting data
    into the dimension tables necessary for representing the metadata of the
    songs

    Parameters
    ==========
    spark : :py:class:`pyspark.sql.SparkSession`
        The instatiated SparkSession object that is enabled to interact with S3
    input_data : str
        The S3 bucket containing the input song metadata and songplay log data
    output_data : str
        Output S3 bucket where to dump the files corresponding to the output
        tables
    """
    # get filepath to song data file
    # we need those wildcards to recurse over all the subdirectories
    # you can't just pass {input_data}/song_data/ - it gives me empty df
    path_to_song_data = f"{input_data}/song_data/*/*/*/*"
    
    # read song data file
    song_data = spark.read.json(path_to_song_data)

    # extract columns to create songs table, and reduce duplicate values of song
    # ids
    songs_table = song_data.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ).dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    outPath = f"{output_data}/songs"
    songs_table.write.parquet(
            outPath, 
            partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = song_data.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ).dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    outPath = f"{output_data}/artists"
    artists_table.write.parquet(outPath)


def process_log_data(spark, input_data, output_data):
    """This function loads the songplay logfiles that holds the events of
    songplays. This generates the songplays fact table.
    """
    # get filepath to log data file
    path_to_log_data = f"{input_data}/log_data/2018/11/*"

    # read log data file
    log_data = spark.read.json(path_to_log_data)

    path_to_song_data = f"{input_data}/song_data/*/*/*/*"
    
    # read song data file
    song_data = spark.read.json(path_to_song_data)

    # extract columns to create songs table, and reduce duplicate values of song
    # ids
    songs_table = song_data.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ).dropDuplicates(subset=['song_id'])

    # extract columns to create artists table
    artists_table = song_data.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ).dropDuplicates(subset=['artist_id'])
    
    # filter by actions for song plays
    nextSong_log_df = log_data.filter("page='NextSong'")

    # extract columns for users table    
    users_table = nextSong_log_df.select(
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ).dropDuplicates(subset=['userId'])
    
    # write users table to parquet files
    outPath = f"{output_data}/users"
    users_table.write.parquet(outPath)

    # create timestamp column from original timestamp column
    nextSong_log_df = nextSong_log_df.withColumn("start_time", to_timestamp(from_unixtime(col("ts")/1000.0)))\
                                    .withColumn("hour", hour(col("start_time")))\
                                    .withColumn("day", dayofmonth(col("start_time")))\
                                    .withColumn("week", weekofyear(col("start_time")))\
                                    .withColumn("month", month(col("start_time")))\
                                    .withColumn("year", year(col("start_time")))\
                                    .withColumn("weekday", dayofweek(col("start_time")))
    # extract columns to create time table
    time_table = nextSong_log_df.select(
                                    "start_time",
                                    "hour",
                                    "day",
                                    "week",
                                    "month",
                                    "year",
                                    "weekday"
    )
    
    # write time table to parquet files partitioned by year and month
    outPath = f"{output_data}/time"
    time_table.write.parquet(outPath, partitionBy=["year", "month"])

    artists_and_songs_table = artists_table.join(
        songs_table.withColumnRenamed("year", "song_release_year"),
        on="artist_id",
        how="right"
    )
    log_data_with_artist_and_song_data = nextSong_log_df.join(
        artists_and_songs_table,
        (nextSong_log_df.artist==artists_and_songs_table.artist_name) & (nextSong_log_df.song==artists_and_songs_table.title),
        how="left")
    log_data_with_artist_and_song_data.printSchema()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data_with_artist_and_song_data.select(
        "start_time", 
        "userId",
        "level",
        "song_id",
        "artist_id",
        "sessionId",
        "location",
        "userAgent",
        "year",
        "month"
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    outPath = f"{output_data}/songplays"
    songplays_table.write.parquet(
            outPath,
            partitionBy=["year","month"]
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-data-engineer-data-lake-project"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
