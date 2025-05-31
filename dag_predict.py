from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import requests
import json

'''
This DAG is designed to run a prediction task after the completion of a data pipeline DAG.
It waits for the data pipeline to finish before making a prediction request to a specified endpoint.
'''

def make_prediction():
    url = "http://13.228.168.101:9000/predict/next"
    headers = {"Content-Type": "application/json"}
    data = {"forecast_horizon": 36}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        raise Exception(f"Prediction request failed with status code {response.status_code}: {response.text}")
    return response.json()

with DAG(
    dag_id='BAQ-prediction-pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2024, 4, 25),
    schedule_interval='@daily',
    catchup=False,
    tags=['prediction_pipeline']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Wait for the data pipeline DAG to complete
    wait_for_data_pipeline = ExternalTaskSensor(
        task_id='wait_for_data_pipeline',
        external_dag_id='BAQ-data-pipeline',
        external_task_id='end',
        timeout=3600,  # 1 hour timeout
        mode='reschedule',  # Reschedule if the upstream DAG isn't done yet
    )

    predict_task = PythonOperator(
        task_id='make_prediction',
        python_callable=make_prediction
    )

    # Set task dependencies
    start >> wait_for_data_pipeline >> predict_task >> end
