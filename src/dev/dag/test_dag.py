from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import os
from upload_to_bq import bq_load
from file_extension_check import file_extension_check
from schema_validation import schema_valid

def get_name(**context):
    return(context["dag_run"].conf["name"])
        
with DAG("test_dag", start_date=datetime(2022,11,23),catchup=False,schedule_interval=None) as dag:
    #catchup false means it will not run pending dags from start date
    bqload=PythonOperator(
        task_id="upload_to_bq",
        python_callable=bq_load.insert_func
    )

    check_ext=ShortCircuitOperator(
        task_id="check_extension",
        provide_context=True,
        python_callable=file_extension_check.check_ext,
    )
    
    schema_validation=ShortCircuitOperator(
        task_id="validate_schema",
        provide_context=True,
        python_callable=schema_valid.validation,
    )




    return_file_name = PythonOperator(
        task_id='return_file_name', python_callable=get_name, provide_context=True, do_xcom_push=True)
    
    return_file_name>>check_ext>>schema_validation>>bqload