import requests
import json
import random
import time
import os
from datetime import datetime

# Hardcoded Slack webhook URL
WEBHOOK_URL = "https://hooks.slack.com/services/YOURBIGLONGKEYINSLACKasdf"
# jokes.txt is assumed to be in the same directory as this script
JOKES_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jokes.txt')

def run_dadjokes(webhook_url, jokes_file_path):
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
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
        print(f"The file '{jokes_file_path}' was not found. Please make sure it exists.")
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    run_dadjokes(WEBHOOK_URL, JOKES_FILE_PATH)