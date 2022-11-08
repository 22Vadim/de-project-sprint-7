import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval=None,
)

mart_1 = SparkSubmitOperator(
    task_id="mart_1",
    dag=dag_spark,
    application="/home/vsmirnov22/mart_1.py",
    conn_id="yarn_spark",
    application_args=[
        "/user/master/data/geo/events",
        "/user/vsmirnov22/data/geo2.csv",
        "2022-05-31",
        "/user/vsmirnov22/data/analitycs/mart_1"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory="1g",
)

mart_2 = SparkSubmitOperator(
    task_id="mart_2",
    dag=dag_spark,
    application="/home/vsmirnov22/mart_2.py",
    conn_id="yarn_spark",
    application_args=[
        "/user/master/data/geo/events",
        "/user/vsmirnov22/data/analitycs/mart_2",
        "2022-05-31"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory="1g",
)

mart_3 = SparkSubmitOperator(
    task_id="mart_3",
    dag=dag_spark,
    application="/home/vsmirnov22/mart_3.py",
    conn_id="yarn_spark",
    application_args=[
        "/user/master/data/geo/events",
        "/user/vsmirnov22/data/analitycs/mart_1/df_local_time",
        "/user/vsmirnov22/data/geo2.csv",
        "/user/vsmirnov22/data/analitycs/mart_3",
        "2022-05-31"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=1,
    executor_memory="1g",
)


mart_1 >> mart_2 >> mart_3
