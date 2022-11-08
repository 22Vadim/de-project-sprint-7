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

# Step 4 Friends mart
def df_friends(df_city, df_local_time):

    window = Window().partitionBy('user').orderBy('date')
    df_city_from = df_city.selectExpr('event.message_from as user','lat_double_fin', 'lng_double_fin', 'date')
    df_city_to = df_city.selectExpr('event.message_to as user','lat_double_fin', 'lng_double_fin', 'date')
    df = (df_city_from.union(df_city_to)
                    .select(F.col('user'), 
                            F.col('date'),
                            F.last(F.col('lat_double_fin'),ignorenulls = True)
                    .over(window).alias('lat_to'), F.last(F.col('lng_double_fin'), ignorenulls = True)
                    .over(window).alias('lng_to') )
        )
  
    window = Window().partitionBy('event.message_from', 'week', 'event.message_to')
    window_rn = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
    window_friend = Window().partitionBy('event.message_from')

    df_friends = (df_city.withColumn('week', F.trunc(F.col('date'), 'week'))
                         .withColumn('cnt', F.count('*').over(window))
                         .withColumn('rn', F.row_number().over(window_rn))
                         .filter(F.col('rn') <= 5)
                         .join(df, (df.user == df_city.event.message_to) & (df.date==df_city.date), 'left')
                         .withColumn('diff', F.acos(F.sin(F.col('lat_double_fin')) 
                                                   * F.sin(F.col('lat_to')) 
                                                   + F.cos(F.col('lat_double_fin'))
                                                   * F.cos(F.col('lat_to'))
                                                   * F.cos(F.col('lng_double_fin')-F.col('lng_to'))
                                                   )
                                             *F.lit(6371)
                                     )
                         .filter(F.col('diff') <= 1)
                         .withColumn('friends', F.collect_list('event.message_to').over(window_friend))
                         .join(df_local_time, 'city', 'inner')
                         .selectExpr('event.message_from as user', 'friends', 'local_time' , 'TIME as time_UTC', 'week', 'city')
    )
    return df_friends

def main():
    df_city_path = sys.argv[1]
    df_local_time_path = sys.argv[2]
    destination_path = sys.argv[3]
    date = sys.argv[4]

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

    df_friends.write.parquet(destination_path + f'df_friends')

if __name__ == "__main__":
        main()
