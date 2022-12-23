from datetime import datetime,timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'retries': 1,
    'start_date':datetime(2022,12,21)
}

dag = DAG(
    'worldcup_pyspark_analysis',
    default_args = default_args
)

run_extraction_pyspark = SparkSubmitOperator(
    task_id = 'run_extraction_pyspark',
    dag=dag,
    conn_id = 'spark_local',
    jars='/include/aws-java-sdk-1.12.368.jar,/include/aws-java-sdk-bundle-1.12.368.jar,/include/hadoop-aws-3.2.2.jar',
    executor_cores=2,
    executor_memory='2g',
    driver_memory='2g',
    application = '/app/01_ExtractingDatasets.py'
)

display_raw_data = BashOperator(
    task_id='display_raw_data',
    bash_command='ls -l /app/data/raw/',
    dag=dag
)

run_transform_pyspark = SparkSubmitOperator(
    task_id = 'run_transform_pyspark',
    dag=dag,
    conn_id = 'spark_local',
    jars='/include/aws-java-sdk-1.12.368.jar,/include/aws-java-sdk-bundle-1.12.368.jar,/include/hadoop-aws-3.2.2.jar',
    executor_cores=2,
    executor_memory='2g',
    driver_memory='2g',
    application = '/app/02_TransformingDatasets.py'
)

display_transformed_data = BashOperator(
    task_id='display_transformed_data',
    bash_command='ls -l /app/data/transformed/',
    dag=dag
)

run_loading_pyspark = SparkSubmitOperator(
    task_id = 'run_loading_pyspark',
    dag=dag,
    conn_id = 'spark_local',
    jars='/include/aws-java-sdk-1.12.368.jar,/include/aws-java-sdk-bundle-1.12.368.jar,/include/hadoop-aws-3.2.2.jar',
    executor_cores=2,
    executor_memory='2g',
    driver_memory='2g',
    application = '/app/03_LoadingFinalDatasets.py'
)

run_extraction_pyspark >> display_raw_data
display_raw_data >> run_transform_pyspark
run_transform_pyspark >> display_transformed_data
display_transformed_data >> run_loading_pyspark
