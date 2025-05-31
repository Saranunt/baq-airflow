from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import json

'''
This DAG is designed to run a batch prediction task daily.
It makes a prediction request to a specified endpoint without waiting for any upstream tasks.
It is scheduled to run daily at 2:00 AM.
Mainly because the previous DAG was not working as expected. XD
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
    dag_id='batch_prediction',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2024, 4, 25),
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    catchup=False,
    tags=['prediction_pipeline']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    predict_task = PythonOperator(
        task_id='make_prediction',
        python_callable=make_prediction
    )

    # Set task dependencies
    start >> predict_task >> end
