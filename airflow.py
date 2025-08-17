from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


import subprocess
from airflow.exceptions import AirflowException




 #def run_spark_job():
   # subprocess.run(["spark-submit", "--master", "yarn", "/home/talentum/proj/transform.py"], check=True)

default_args = {
    'owner': 'debasis',
    'start_date': datetime(2025, 7, 27),
    'retries': 1,
}

with DAG('airflow',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # 1. create CSV to HDFS
    create_dir_hdfs=BashOperator(
        task_id='create_dir_hdfs',
        bash_command='hdfs dfs -mkdir -p /user/etl/raw'
    )


    upload_to_hdfs = BashOperator(
        task_id='upload_to_hdfs',
        bash_command='hdfs dfs -copyFromLocal -f /home/talentum/proj/india_housing_prices.csv /user/etl/raw/'
    )

    # 2. Create Hive staging table
    create_hive_staging = BashOperator(
        task_id='create_hive_staging',
        bash_command='hive -f  /home/talentum/proj/create_hive_staging.hive'
    )

    # 3. Create Hive external processed table
    create_hive_processed = BashOperator(
        task_id='create_hive_processed',
        bash_command='hive -f  /home/talentum/proj/housing_processed.hive'
    )
    
    sparksub =BashOperator(
    task_id='sparksub',
    bash_command='python3 /home/talentum/proj/transform.py',
    dag=dag
    )
    create_dir_hdfs >>upload_to_hdfs >> create_hive_staging >> sparksub >> create_hive_processed 
