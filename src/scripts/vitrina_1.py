
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

# Step 2 Created mart sliced by users

def event_with_city(geo_events_source: str, geo_cities: str, spark: SparkSession) -> DataFrame:
    events_message_geo = (spark.read.parquet(geo_events_source)
                               .sample(0.05)
                               .where('event.message_channel_to is null and lat is not null')
                               .where('event_type=="message"')
                               .withColumn('user_id', F.col('event.message_from'))
                               .withColumnRenamed('lat', 'm_lat')
                               .withColumnRenamed('lon', 'm_lon')
                               .withColumn('event_id', F.monotonically_increasing_id())
                         )
    geo_city = (spark.read.parquet(geo_cities)
                                .withColumnRenamed('lat', 'c_lat')
                                .withColumnRenamed('lng', 'c_lon')
               )
    diff = (2 * F.lit(6371) 
              * F.asin(F.sqrt(
                            F.pow(F.sin(F.radians((F.col('m_lat') - F.col('c_lat'))) / 2), 2) 
                            + F.cos(F.radians(F.col('m_lat'))) * F.cos(F.radians(F.col('c_lat')))
                            * F.pow(F.sin(F.radians((F.col('m_lon') - F.col('c_lon'))) / 2), 2)
                             )
                      )
           )
    events_city = (events_message_geo.crossJoin(geo_city)
                                     .withColumn('diff', diff)
                                     .drop('m_lat', 'm_lon', 'c_lat', 'c_lon')
                                     .persist()
                  )

    window = Window().partitionBy('event.message_from').orderBy(F.col('diff').desc())
    df_city = (events_city.withColumn('rn', F.row_number().over(window))
                          .filter(F.col('rn')==1)
                          .drop('rn')
              )
    return df_city

def actual_geo(df_city: DataFrame) -> DataFrame:
    window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
    df_actual_geo = (df_city.withColumn('rn', F.row_number().over(window))
                        .filter(F.col('rn')==1)
                        .selectExpr('event.message_from as user', 'city', 'id as city_id')
                        .persist()
                )
    return df_actual_geo

def act_city(df_city: DataFrame) -> DataFrame:
    window = Window().partitionBy('event.message_from', 'id').orderBy(F.col('date'))
    df_city_travel = (df_city.withColumn('dense_rank', F.dense_rank().over(window))
                             .withColumn('date_diff', F.datediff(F.col('date').cast('date'), F.to_date(F.col('dense_rank').cast('string'), 'dd')))
                             .selectExpr('date_diff', 'event.message_from as user', 'date', 'id')
                             .groupBy('user', 'date_diff', 'id')
                             .agg(F.countDistinct(F.col('date')).alias('cnt_city'))
                )
    return df_city_travel

def home_city(df_city_travel: DataFrame) -> DataFrame:
    df_home_city = (df_city_travel.withColumn('max_dt', F.max(F.col('date_diff'))
                                  .over(Window().partitionBy('user')))
                                  .filter((F.col('cnt_city')>27) & (F.col('date_diff') == F.col('max_dt')))
                                  .persist()
              )
    return df_home_city

def df_local_time(df_city: DataFrame) -> DataFrame:
    df_local_time = (df_city.withColumn('TIME', F.col('event.datetime').cast('Timestamp'))
                            .withColumn('timezone', F.concat(F.lit('Australia'), F.col('city')))
                            .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone')))
                            .select('TIME', 'local_time', 'city')
                    )                        
    return df_local_time

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
    df_city.write.parquet(destination_path + f'df_city/date={date}')

    df_actual_geo = actual_geo(df_city)
    df_actual_geo.write.parquet(destination_path + f'df_actual_geo/date={date}')

    df_city_travel = act_city(df_city)
    df_city_travel.write.parquet(destination_path + f'df_city_travel/date={date}')

    df_home_city = home_city(df_city_travel)
    df_home_city.write.parquet(destination_path + f'df_home_city/date={date}')
    
    df_local_time = df_local_time(df_city)
    df_local_time.write.parquet(destination_path + f'df_local_time/date={date}')


if __name__ == "__main__":
        main()
