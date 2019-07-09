import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType
from pyspark.sql import functions as F 
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

# added header ['credentials'] on cfg file
os.environ['AWS_ACCESS_KEY_ID']=config['credentials']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['credentials']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """create spark session, and load jar file to interact with hadoop on aws."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """load song data files from s3, insert fields into songs table and artists table, and save data in parquet back to S3."""
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    
    # read song data file
    df_song = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df_song.select(["title", "artist_id","year", "duration"]).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df_song.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", 
                                    "artist_longitude as longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')
    
    
def process_log_data(spark, input_data, output_data):
    """load log data files from s3, insert fields into users table, songs table, and songplay table, and save data in parquet back to S3."""
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df_log = spark.read.json(log_data)
 
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_table = df_log.select(col('firstName').alias('first_name'), 
                            col('lastName').alias('last_name'), 
                            col('gender'), 
                            col('level'), 
                            col('userId').alias('user_id')).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users/')
    
    #create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    df_log.printSchema()

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_datetime(x), TimestampType())
    df_log = df_log.withColumn('start_time', get_timestamp(df_log.ts))
    df_log.printSchema()


    # extract columns to create time table
    df_log = df_log.withColumn("hour", F.hour("timestamp"))        \
                   .withColumn("day", F.dayofweek("timestamp"))    \
                   .withColumn("week", F.weekofyear("timestamp"))  \
                   .withColumn("month", F.month("timestamp"))      \
                   .withColumn("year", F.year("timestamp"))        \
                   .withColumn("weekday", F.dayofweek("timestamp")) 
    time_table = df_log.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
     # write time table to parquet files
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time/')
                    
    # read in song data to use for songplays table
    df_log.createOrReplaceTempView("view_log")

    df_song = df_song.drop('year')
    df_song.createOrReplaceTempView("view_song")
                    
        
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """select distinct view_log.start_time, view_log.userId as user_id, view_log.level, view_log.sessionId as session_id, 
        view_log.location, view_log.userAgent as user_agent, view_song.song_id, view_song.artist_id, view_time.year, view_time.month
        from view_log
            inner join 
        view_song 
            on view_log.artist = view_song.artist_name and 
            view_log.song = view_song.title and
            view_log.length = view_song.duration
            inner join 
        view_time 
            on view_log.start_time = view_time.start_time
        """
    )               
                                  
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())                  
                    
    # write songplays table to parquet files
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays/')  

                 
                    
def main():
    """load data from s3, create 5 tables, and save data in parquet format back to s3. """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend-michlin0825/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)    

if __name__ == "__main__":
    main()
