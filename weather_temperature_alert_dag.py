from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'email_dag_example',
    default_args=default_args,
    schedule_interval=timedelta(hours=24),
    catchup=False
)

def connect_to_data_source(**kwargs):
    # API endpoint for weather data
    endpoint = 'http://api.openweathermap.org/data/2.5/weather'
    # API key
    api_key = 'your_api_key'
    # City and country code
    city = 'London,UK'
    
    # Construct the API request
    url = f'{endpoint}?q={city}&appid={api_key}'
    
    # Send the API request
    response = requests.get(url)
    
    # Check for a successful response
    if response.status_code == 200:
        # Extract the weather data
        data = response.json()
        return data
    else:
        raise Exception(f'Request failed with status code: {response.status_code}')

def check_temperature(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='connect_to_data_source')
    temperature = data['main']['temp']
    threshold = 280.0 # The temperature threshold in Kelvin
    if temperature > threshold:
        return True
    return False

def send_email(**kwargs):
    temperature_above_threshold = kwargs['ti'].xcom_pull(task_ids='check_temperature')
    if temperature_above_threshold:
        email_operator = EmailOperator(
            task_id='send_email',
            to='youremail@example.com',
            subject='High Temperature Alert',
            html_content="The temperature is above the threshold.",
            dag=dag
        )
        email_operator.execute(context=kwargs)

connect_to_data_source_task = PythonOperator(
    task_id='connect_to_data_source',
    python_callable=connect_to_data_source,
    provide_context=True,
    dag=dag
)

check_temperature_task = PythonOperator(
    task_id='check_temperature',
    python_callable=check_temperature,
    provide_context=True,
    dag=dag
)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    provide_context=True,
    dag=dag,
    trigger_rule='one_success' # Only run if the previous task (check_temperature) succeeds
)

Set the task dependencies
connect_to_data_source_task >> check_temperature_task >> send_email_task
