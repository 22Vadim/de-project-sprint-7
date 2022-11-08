import os
import sys
import datetime

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

def event_with_city(geo_events_source: str, geo_cities: str, spark: SparkSession) -> DataFrame:

    events = spark.read.parquet(geo_events_source)

    events_day = events.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull() & (events.event_type == "message"))

    event_day =  events_day.select(F.col('event.message_from'), 
                                F.col('event.datetime'),
                                F.col('event.message_id'), 
                                F.col('date'), 
                                F.col('lat'),  
                                F.col('lon'))

    geo = spark.read.csv("/user/vsmirnov22/data/geo.csv", sep =';', header = True)

    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                     .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                     .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    new = event_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                        + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                        *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))


    window = Window().partitionBy('message_from', "message_id")

    new_2 = new.withColumn("min_km", F.min('km').over(window)) \
                        .select('message_from', 'datetime', 'message_id', 'date', 'city', 'km', 'min_km')


    new_3 = new_2.withColumn( "city_event", F.when(new_2.min_km == new_2.km, new_2.city) \
                            .otherwise('NOT')) \
                            .filter(F.col('city_event') != 'NOT')

    window = Window().partitionBy('message_from', "city_event").orderBy('date')
    new_4 = new_3.withColumn('days_in', F.dense_rank().over(window))

    window = Window().partitionBy('message_from', "city_event")
    new_5 = new_4.withColumn('days_max', F.max('days_in').over(window))
    new_6 = new_5.withColumn( "city_home", F.when(new_5.days_max > 26 , new_5.city).otherwise('NOT'))

    window = Window().partitionBy('message_from').orderBy('city_event')
    new_7 = new_6.withColumn('city_cnt', F.dense_rank().over(window))

    window = Window().partitionBy('message_from')
    new_8 = new_7.withColumn('city_cnt_max', F.max('city_cnt').over(window)). \
                            select('message_from', 'datetime', 
                            'date', 'city_event', 
                             'km', 'days_in', 
                             'days_max', 'city_home', 
                             'city_cnt', 'city_cnt_max').persist()

    dist_geo = new_8.select(F.col('message_from').alias('message_from_2'), 'city_event').distinct()
    df = (dist_geo.withColumn('lst', F.col('city_event').alias('lst'))
                             .groupBy('message_from_2')
                             .agg( F.concat_ws(',', F.collect_list('lst').alias('b_list')).alias('travel_array')))

    join_df = new_8.join(df, new_8.message_from == df.message_from_2, how="inner").drop('message_from_2') \
                            .select(F.col('message_from').alias('user_id'), 
                            F.col('city_event').alias('act_city'), 
                            F.col('city_home').alias('home_city'), 
                            F.col('city_cnt_max').alias('travel_count'), 
                            'travel_array', 'datetime')      

    df_with_time =  join_df.withColumn('TIME', F.col('datetime').cast('Timestamp')) \
                    .withColumn('timezone', F.concat(F.lit('Australia'), F.lit('/'),  F.col('act_city'))) \
                    .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone'))).drop('datetime')

    return df_with_time


def df_local_time(df_with_time: DataFrame) -> DataFrame:
    local_tyme = df_with_time.select(F.col('act_city').alis('city'), 'timezone', 'local_time')

    return local_tyme




def main():
    geo_events_source = sys.argv[1]
    geo_cities = sys.argv[2]
    date = sys.argv[3]
    destination_path = sys.argv[4]

    spark = (SparkSession.builder
                        .master('yarn')
                        .config('spark.driver.memory', '1g')
                        .config('spark.driver.cores', 2)
                        .appName('sliced_by_user')
                        .getOrCreate())
    
    df_city = event_with_city(geo_events_source, geo_cities, spark)
    df_city.write.parquet(destination_path + f'df_city')


    calc_user_local_time = df_local_time(df_with_time)
    calc_user_local_time.write.parquet(destination_path + f'df_local_time')


if __name__ == "__main__":
        main()    
