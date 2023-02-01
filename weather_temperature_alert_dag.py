from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import base64
import json
from datetime import datetime, timedelta
import requests
from random import randint
from datetime import datetime

openweathermap_api_key = '532e673cf745e7ca2866f84bc0782f8e'
openweathermap_city = 'Tel Aviv, IL'
mailjet_api_key = '9dc36b6e873dae54064480583a8f8a29'
mailjet_secret_key = '99f40a0b993e12c1e4c10a0ee186dc86'


def _connect_to_data_source(ti):
    endpoint = 'http://api.openweathermap.org/data/2.5/weather'
    city = openweathermap_city
    url = f'{endpoint}?q={city}&appid={openweathermap_api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(data)
        return data
    else:
        raise Exception(f'Request failed with status code: {response.status_code}')


def _check_temperature_task(task_instance):
    response = task_instance.xcom_pull(task_ids='connect_to_data_source_task')
    main_weather = response['weather'][0]['main']
    print(main_weather)
    return 'Clouds' in main_weather


def _send_email_task(task_instance):
    is_raining = task_instance.xcom_pull(task_ids='check_temperature_task')
    if not is_raining:
        return
    message = f'{mailjet_api_key}:{mailjet_secret_key}'
    sample_string_bytes = message.encode("ascii")
    base64_bytes = base64.b64encode(sample_string_bytes)
    base64_string = base64_bytes.decode("ascii")

    url = "https://api.mailjet.com/v3.1/send"

    payload = json.dumps({
        "Messages": [
            {
                "From": {
                    "Email": "oferusa@gmail.com",
                    "Name": "Your Mailjet Pilot"
                },
                "To": [
                    {
                        "Email": "oferusa@gmail.com",
                        "Name": "Passenger 1"
                    }
                ],
                "Subject": "Your email flight plan!",
                "TextPart": "Dear passenger, welcome to Mailjet! May the delivery force be with you!"
            }
        ]
    })

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64_string}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)


with DAG("weather_temperature_alert_dag", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    connect_to_data_source_task = PythonOperator(
        task_id="connect_to_data_source_task",
        python_callable=_connect_to_data_source
    )

    check_temperature_task = PythonOperator(
        task_id="check_temperature_task",
        python_callable=_check_temperature_task
    )

    send_email_task = PythonOperator(
        task_id="send_email_task",
        python_callable=_send_email_task
    )

    connect_to_data_source_task >> check_temperature_task >> send_email_task
