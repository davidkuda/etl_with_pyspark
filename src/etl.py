import configparser
from datetime import datetime
import os

import boto3
import pyspark.sql.session
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, \
    DoubleType, IntegerType, TimestampType, LongType


def get_config(path: str = 'dl.cfg'):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def set_env_vars():
    config = get_config()
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def create_bucket_if_not_exists(bucket_name: str):
    config = get_config()
    aws_access_key_id = config['AWS']['aws_access_key_id']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region_name = 'us-east-1'

    aws_session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)

    s3 = aws_session.client('s3')

    existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]

    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)
        # TODO: Catch errors such as invalid bucket name, invalid characters or alike.


class SparkETL:
    def __init__(self, session: pyspark.sql.session.SparkSession, src: str, dst: str):
        self.spark = session
        self.src = src
        self.dst = dst

    def load_song_data(self):
        src_path_song_data = self.src + 'song_data/*/*/*/*.json'

        schema_song_data = StructType(
            [StructField('artist_id', StringType(), True),
             StructField('artist_latitude', DoubleType(), True),
             StructField('artist_location', StringType(), True),
             StructField('artist_longitude', DoubleType(), True),
             StructField('artist_name', StringType(), True),
             StructField('duration', DoubleType(), True),
             StructField('num_songs', LongType(), True),
             StructField('song_id', StringType(), True),
             StructField('title', StringType(), True),
             StructField('year', LongType(), True)])

        # read song data file
        df = self.spark.read.schema(schema_song_data).json(src_path_song_data)
        return df

    def load_log_data(self):
        src_path_log_data = self.src + 'log_data/*/*/*.json'

        schema_log_data = StructType(
            [StructField('artist', StringType(), True),
             StructField('auth', StringType(), True),
             StructField('firstName', StringType(), True),
             StructField('gender', StringType(), True),
             StructField('itemInSession', LongType(), True),
             StructField('lastName', StringType(), True),
             StructField('length', DoubleType(), True),
             StructField('level', StringType(), True),
             StructField('location', StringType(), True),
             StructField('method', StringType(), True),
             StructField('page', StringType(), True),
             StructField('registration', DoubleType(), True),
             StructField('sessionId', LongType(), True),
             StructField('song', StringType(), True),
             StructField('status', LongType(), True),
             StructField('ts', LongType(), True),
             StructField('userAgent', StringType(), True),
             StructField('userId', StringType(), True)])

        df = self.spark.read.schema(schema_log_data).json(src_path_log_data)
        return df

    def process_song_data(self) -> None:
        """Splits the song data into the tables songs and artists. Writes each
        table to a parquet file to the path specified in self.dst."""
        df = self.load_song_data()

        # extract columns to create songs table
        songs_table = (df.select("song_id", "title", "artist_id",
                                 "year", "duration")
                       .dropDuplicates(["song_id"]))

        # write songs table to parquet files partitioned by year and artist
        songs_table.write.parquet(self.dst + "songs_table.parquet",
                                  mode="overwrite",
                                  partitionBy=["year", "artist_id"])

        # extract columns to create artists table
        artists_table = (df.select("artist_id",
                                   col("artist_name").alias("name"),
                                   col("artist_location").alias("location"),
                                   col("artist_latitude").alias("latitude"),
                                   col("artist_longitude").alias("longitude"))
                         .distinct())

        # write artists table to parquet files
        artists_table.write.parquet(self.dst + "artists_table.parquet",
                                    mode="overwrite")

    def process_log_data(self) -> None:
        """Splits the log data into the tables users and time. Writes each
        table to a parquet file to the path specified in self.dst."""
        df = self.load_log_data()

        # extract columns for users table
        users_table = (df
                       .select(col("userId").alias("user_id"),
                               col("firstName").alias("first_name"),
                               col("lastName").alias("last_name"),
                               col("gender"),
                               col("level"),
                               col("ts"))
                       .orderBy(col("user_id"),
                                col("ts").desc()))

        # write users table to parquet files
        users_table.write.parquet(self.dst + "users_table.parquet",
                                  mode="overwrite")

        # Parse datetime with a UDF
        @udf(returnType=TimestampType())
        def parse_timestamp(ts):
            parsed_ts = datetime.fromtimestamp(ts / 1000).replace(microsecond=0)
            return parsed_ts

        # extract columns to create time table
        time_table = (df
                      .withColumn('start_time', parse_timestamp('ts'))
                      .select('start_time')
                      .withColumn('week', weekofyear('start_time'))
                      .withColumn('hour', hour('start_time'))
                      .withColumn('day', dayofmonth('start_time'))
                      .withColumn('month', month('start_time'))
                      .withColumn('year', year('start_time')))

        # write time table to parquet files partitioned by year and month
        time_table.write.parquet(self.dst + "time_table.parquet",
                                 mode="overwrite")

    def create_songplays_table(self):
        df_log_data = self.load_log_data()
        df_song_data = self.load_song_data()

        # Parse datetime with a UDF
        @udf(returnType=TimestampType())
        def parse_timestamp(ts):
            parsed_ts = datetime.fromtimestamp(ts / 1000).replace(microsecond=0)
            return parsed_ts

        # extract columns from joined song and log datasets to create songplays table
        songplays_table = (df_song_data
                           .join(df_log_data,
                                 df_song_data.artist_name == df_log_data.artist)
                           .withColumn('start_time',
                                       parse_timestamp(df_log_data.ts))
                           .select(col('userId').alias('user_id'),
                                   col('level'),
                                   col('song_id'),
                                   col('artist_id'),
                                   col('sessionId'),
                                   col('location'),
                                   col('userAgent'))
                           .where(df_log_data.page == 'NextSong'))

        # write songplays table to parquet files partitioned by year and month
        songplays_table.write.parquet(self.dst + 'songplays.parquet',
                                      mode='overwrite')


def main():
    set_env_vars()
    spark = create_spark_session()
    src_s3_path = "s3a://udacity-dend/"
    dst_bucket_name = "spark-data-lake-etl"
    dst_s3_path = f"s3a://{dst_bucket_name}"

    create_bucket_if_not_exists(dst_bucket_name)

    spark_etl = SparkETL(spark, src_s3_path, dst_s3_path)
    spark_etl.process_song_data()
    spark_etl.process_log_data()
    spark_etl.create_songplays_table()


if __name__ == "__main__":
    main()
