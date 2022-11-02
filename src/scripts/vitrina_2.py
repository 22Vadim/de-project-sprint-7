import os
import sys
import datetime
from dags.marts_update import DESTINATION

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

# Step 3 Zones slicing
def zones_slicing(df_city):
    window_week = Window().partitionBy('week')
    window_month = Window().partitionBy('month')
    window = Window().partitionBy('event.message_from').orderBy(F.col('date'))

    df_zones_slicing = (df_city.withColumn('month',F.trunc(F.col('date'), 'month'))
                               .withColumn('week',F.trunc(F.col('date'), 'week'))
                               .withColumn('rn',F.row_number().over(window))
                               .withColumn('week_message',F.sum(F.when(df_city.event_type == 'message',1).otherwise(0)).over(window_week))
                               .withColumn('week_reaction',F.sum(F.when(df_city.event_type == 'reaction',1).otherwise(0)).over(window_week))
                               .withColumn('week_subscription',F.sum(F.when(df_city.event_type == 'subscription',1).otherwise(0)).over(window_week))
                               .withColumn('week_user',F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_week))
                               .withColumn('month_message',F.sum(F.when(df_city.event_type == 'message',1).otherwise(0)).over(window_month))
                               .withColumn('month_reaction',F.sum(F.when(df_city.event_type == 'reaction',1).otherwise(0)).over(window_month))
                               .withColumn('month_subscription',F.sum(F.when(df_city.event_type == 'subscription',1).otherwise(0)).over(window_month))
                               .withColumn('month_user',F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_month))
                               .drop('rn')
        )
    return df_zones_slicing

def main():
    df_city_path = sys.argv[1]
    destination_path = sys.argv[2]
    date = sys.argv[3]

    spark = (SparkSession.builder
                        .master('yarn')
                        .config('spark.driver.memory', '1g')
                        .config('spark.driver.cores', 2)
                        .appName('sliced_by_zones')
                        .getOrCreate())
    
    df_city = (spark.read.parquet(df_city_path)
               )

    df_zones_slicing = zones_slicing(df_city)

    df_zones_slicing.write.parquet(destination_path + f'zones_slicing/date={date}')

if __name__ == "__main__":
        main()
