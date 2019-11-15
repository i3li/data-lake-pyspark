import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create -if needed- and return a spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data files, process them and then write the result into parquet files
    
    Parameters:
    spark: The spark session
    input_data: The path of the input data. **The path must end with /**
    output_data: The path where the output parquet files are written into. **The path must end with /**
    """
    
    # get filepath to song data file
    # s3a://udacity-dend/song_data/*/*/*/*.json
    song_data = 'song_data/A/B/C/*.json'
    
    # read song data file
    df = spark.read.json(input_data + song_data)

    # create a view to the song file
    df.createOrReplaceTempView("song_table")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT 
    song_id, title, artist_id, INT(year), duration
    FROM song_table
    """).distinct()
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
    .partitionBy("year", "artist_id") \
    .parquet(output_data + 'songs/')

    
    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
    FROM song_table
    """).distinct()
    
    # write artists table to parquet files
    artists_table.write \
    .parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """Read log data files, process them and then write the result into parquet files
    
    Parameters:
    spark: The spark session
    input_data: The path of the input data. **The path must end with /**
    output_data: The path where the output parquet files are written into. **The path must end with /**
    """
    
    # get filepath to log data file
    log_data = 'log_data/2018/11/2018-11-12-events.json' #'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(input_data + log_data)
    
    # filter by actions for song plays
    df = df.select("*").where("page = 'NextSong'")

    # create a view to the log file
    df.createOrReplaceTempView("log_table")
    
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT
    userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level
    FROM log_table
    """).distinct()
    
    # write users table to parquet files
    users_table.write \
    .parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda milli_ts: int(milli_ts / 1000), LongType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
#     get_datetime = udf(lambda s_ts: datetime.fromtimestamp(s_ts), TimestampType())
#     df = df.withColumn("datetime", get_datetime(df.timestamp))
    get_datetime = udf(lambda milli_ts: datetime.fromtimestamp(milli_ts / 1000), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # register a functoin for getting the weekday from the timestamp
    # EXTRACT(weekday) is not supported in pyspark
    spark.udf.register('get_weekday', lambda timestamp: timestamp.weekday())
    
    df.createOrReplaceTempView("log_table")
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT
    datetime AS start_time,
    EXTRACT(hour FROM datetime) AS hour,
    EXTRACT(day FROM datetime) AS day,
    EXTRACT(week FROM datetime) AS week,
    EXTRACT(month FROM datetime) AS month,
    EXTRACT(year FROM datetime) AS year,
    get_weekday(datetime) AS weekday
    FROM log_table
    """).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
    .partitionBy("year", "month") \
    .parquet(output_data + 'time/')

    # get filepath to song data file
    # s3a://udacity-dend/song_data/*/*/*/*.json
    song_data = 'song_data/A/B/C/*.json'
    
    # read in song data to use for songplays table    
    song_df = spark.read.json(output_data + song_data)

    # create a view to the log file
    song_df.createOrReplaceTempView("song_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT
    LT.datetime AS start_time,
    LT.userId AS user_id, 
    LT.level,
    ST.song_id,
    ST.artist_id,
    LT.sessionId AS session_id,
    LT.location,
    LT.userAgent AS user_agent
    FROM song_table ST JOIN log_table LT
    ON ST.title = LT.song
    """).distinct() \
    .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
    .partitionBy("EXTRACT(year FROM start_time)", "EXTRACT(month FROM start_time)") \
    .parquet(output_data + 'songplays/')


def main():
    """The program entry point."""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ali.project4dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
