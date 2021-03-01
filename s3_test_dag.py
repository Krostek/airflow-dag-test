import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import airflow.hooks.S3_hook


def upload_string_to_S3_with_hook(string_to_send, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_s3_conn')
    hook.load_string(string_to_send, key, bucket_name)

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 2, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}
# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('S3_dag_test_v1.2', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_string_to_S3_with_hook,
        op_kwargs={
            'string_to_send': 'Hello AWS S3 from Airflow',
            'key': 'my_S3_file_' + dt.datetime.now().strftime("%m-%d-%Y, %H:%M:%S") + '.txt',
            'bucket_name': 'sparklander-airflow-demo',
        })

    # Use arrows to set dependencies between tasks
    start_task >> upload_to_S3_task