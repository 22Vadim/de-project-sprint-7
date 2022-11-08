import os
import sys
import datetime

import findspark
from scripts.st_2_sliced_by_user import df_local_time
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


def df_friends(df_city, geo_cities, df_local_time):

    events = spark.read.parquet("df_city")

    events_day = events.filter(F.col("lat").isNotNull() 
                           & F.col("lat").isNotNull() & 
                           (events.event_type == "message"))

    geo = spark.read.csv("/user/vsmirnov22/data/geo.csv", sep =';', header = True)

    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                     .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                     .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    new = events_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                      + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                      *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    window = Window().partitionBy('event.message_from', "event.message_id")

    new_2 = new.withColumn("min_km", F.min('km').over(window)).filter(F.col('km') == F.col('min_km')).persist()

    new_3 = new_2.select('event.message_from', 'event.datetime', 'city', 'lat', 'lon').dropDuplicates()

    new_4 = new_3.alias('new_4')

    new_5 = new_4.select(F.col('message_from').alias('message_from_2'),
            F.col('datetime').alias('datetime_2'),        
            F.col('city').alias('city_2'),
            F.col('lat').alias('lat_2'),
            F.col('lon').alias('lon_2'))

    new_6 = new_3.crossJoin(new_5)

    new_7 =  new_6.filter(F.col('message_from') != F.col('message_from_2')).drop('datetime_2')

    new_8 = new_7.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                      + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                      *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    new_8 = new_8.dropDuplicates()

    window = Window().partitionBy('message_from', "message_from_2")

    new_9 = new_8.withColumn("min_km", F.min('km').over(window))

    new_10 = new_9.filter(new_9.km == new_9.min_km).select(F.col('message_from').alias('user_left'),
                                                      F.col('message_from_2').alias('user_right'),
                                                      F.col('city').alias('zone_id'), 'km', 'datetime' ).persist()

    new_11 =     new_10.filter(new_10.km <1 ) \
            .withColumn('processed_dttm', F.current_timestamp()).join(df_local_time, 'city', 'inner')                                                                                                                                                                                          

    df_friends = new_11.select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')
    
    return df_friends

def main():
    df_city_path = sys.argv[1]
    df_local_time_path = sys.argv[2]
    geo_cities = sys.argv[3]
    destination_path = sys.argv[4]
    date = sys.argv[5]

    spark = (SparkSession.builder
                        .master('yarn')
                        .config('spark.driver.memory', '1g')
                        .config('spark.driver.cores', 2)
                        .appName('sliced_by_zones')
                        .getOrCreate())
    
    df_city = (spark.read.parquet(df_city_path)
               )
    df_local_time = (spark.read.parquet(df_local_time_path)
               )

                   

    df_friends = df_friends(df_city, df_local_time)

    df_friends.write.parquet(destination_path + f'df_friends/date={date}')

if __name__ == "__main__":
        main()
