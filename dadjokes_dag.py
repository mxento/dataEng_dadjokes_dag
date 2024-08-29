from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import requests
import json
import random
import time
import os

default_args = {
    'owner': 'michael_xia',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 28),
    'email': ['YOUREMAIL@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DadJokes',
    default_args=default_args,
    schedule_interval='0 * * * *',
    description='dadjokes',
    catchup=False,
    tags=['DadJokes']
)

def run_dadjokes():
    webhook_url = Variable.get("YOUR_SLACK_WEBHOOK_HERE")
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get the directory of the current script
    dag_directory = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the jokes.txt file in the same directory
    jokes_file_path = os.path.join(dag_directory, 'dadjokes.txt')
    
    try:
        # Open the jokes file and read jokes
        with open(jokes_file_path, 'r') as file:
            lines = file.read().splitlines()
        
        # Randomly select a joke
        joke = random.choice(lines)
        joke_parta, joke_partb = joke.split('<>')
        joke_parta_time = f"[{current_datetime}] {joke_parta}"
        joke_partb_time = f"[{current_datetime}] {joke_partb}"

        # Send each part of the joke to Slack
        for part in [joke_parta_time, joke_partb_time]:
            payload = {"text": part}
            response = requests.post(
                webhook_url, 
                data=json.dumps(payload), 
                headers={'Content-Type': 'application/json'}
            )
            
            # Check for errors
            if response.status_code != 200:
                raise ValueError(f'Request to Slack returned an error {response.status_code}, response: {response.text}')
            else:
                print('Message sent successfully!')
            
            # Wait for 5 seconds between the two messages
            time.sleep(5)
    
    except FileNotFoundError:
        print(f"The file 'dadjokes.txt' was not found in the directory {dag_directory}. Please make sure it is in the same directory as this DAG file.")
    except Exception as e:
        print(f'Error: {e}')

# Define the Dummy and Python operators
start_task_dummy = DummyOperator(task_id='start_dummy_task', retries=0, dag=dag)

make_dadjokes = PythonOperator(
    task_id='rundadjokes',
    python_callable=run_dadjokes,
    dag=dag
)

# Set the task dependencies
start_task_dummy >> make_dadjokes