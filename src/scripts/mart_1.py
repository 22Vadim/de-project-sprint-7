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

    events_day = events.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull() & 
                           (events.event_type == "message")).where("date < '2022-03-30'")  

    events_day =  events_day.select(F.col('event.message_from'), 
                               F.col('event.datetime'),
                               F.col('event.message_id'), 
                               F.col('date'), 
                               F.col('lat'),  
                               F.col('lon'))

    geo = spark.read.csv(geo_cities, sep =';', header = True)

    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                     .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                     .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    nnew = events_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                      + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                      *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))


    window = Window().partitionBy('message_from', "message_id")

    new_2 = new.withColumn("min_km", F.min('km').over(window)) \
                    .select('message_from', 'datetime', 'message_id', 'date', 'city', 'lat', 'lon', 'km', 'min_km').persist()


    new_3 = new_2.filter( new_2.km == new_2.min_km).drop('min_km')

    new_4 =  new_3.select('message_from', 'city', 'date').distinct().persist()

    window = Window().partitionBy('message_from', "city").orderBy('date')
    new_5 = new_4.withColumn('rn', F.row_number().over(window))

    new_6 =  new_5.withColumn('diff', F.col('date') - F.col('rn'))

    window = Window().partitionBy('message_from', "city", 'diff')
    new_7 = new_6.withColumn('days_count', F.count('diff').over(window))

    window = Window().partitionBy('message_from').orderBy('city')
    new_8 = new_7.withColumn('city_count', F.dense_rank().over(window))

    window = Window().partitionBy('message_from')
    new_9 = new_8.withColumn('travel_count', F.max('city_count').over(window))

    new_10 = new_9.withColumn( "home_city", F.when(new_9.days_count > 26 , new_9.city).otherwise('NOT'))

    dist_geo = new_8.select(F.col('message_from').alias('message_from_2'),
                        F.col('city').alias('city_2'), 
                        F.col('date').alias('date_2')).orderBy('message_from_2', 'date_2')

    df = (dist_geo.withColumn('lst', F.col('city_2').alias('lst'))
                         .groupBy('message_from_2')
                         .agg( F.concat_ws(',', F.collect_list('lst').alias('b_list')).alias('travel_array')))

    new_11 =  new_10.join(df, new_8.message_from == df.message_from_2, how="inner") \
                        .drop('message_from_2', 'days_count', 'city_count', 'rn').persist()

    window = Window().partitionBy('message_from')
    new_12 = new_11.withColumn('max_date', F.max('date').over(window))        

    new_13 = new_12.withColumn( "act_city", F.when(new_12.max_date == new_12.date, new_5.city).otherwise('NOT')) \
                                    .drop('city', 'diff', 'max_date')


    df_with_time =  new_13.filter(new_13.act_city != "NOT") \
            .withColumn('TIME', F.col('date').cast('Timestamp')) \
            .withColumn('timezone', F.concat(F.lit('Australia'), F.lit('/'),  F.col('act_city')))
            .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone'))).drop('date')


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


    df_local_time = df_local_time(df_with_time)
    df_local_time.write.parquet(destination_path + f'df_local_time')


if __name__ == "__main__":
        main()    
