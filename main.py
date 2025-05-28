from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from getdata_utils import get_data
from datetime import timedelta
from preprocess_utils import preprocess_data
from dateutil.relativedelta import relativedelta
from datetime import datetime

timelabel = datetime.today() - relativedelta(days=1)
timelabel = timelabel.strftime('%Y_%m_%d')

raw_data_source = f's3://soccer-storage/webapp-storage/data/raw/raw_data_{timelabel}.csv'
processed_data_source = f's3://soccer-storage/webapp-storage/data/processed/processed_data_{timelabel}.csv' 

with DAG(
    dag_id='BAQ-data-pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2024, 4, 25),
    schedule_interval='@daily',  # run every day
    catchup=False,
    tags=['data_pipeline']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    get_data_task = PythonOperator(
        task_id='get_data',
        python_callable=get_data
    )

    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )

    # Set task dependencies
    start >> get_data_task >> preprocess_data_task >>end