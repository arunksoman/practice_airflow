from datetime import datetime, timedelta
import csv
import os
import requests
from airflow import DAG
from airflow.decorators import task

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG instance
dag = DAG(
    dag_id='weather_scraper',
    default_args=default_args,
    description='Fetch weather data for Kochi and save to CSV',
    schedule='@hourly',  # Run every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'scraper', 'kochi'],
)

@task(dag=dag)
def fetch_weather():
    """Fetch raw weather data from API"""
    try:
        # Using wttr.in API (no API key required)
        url = "https://wttr.in/Kochi?format=j1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        print("Successfully fetched weather data from API")
        
        # Return the current condition data
        return data['current_condition'][0]
        
    except Exception as e:
        print(f"Error fetching weather data: {str(e)}")
        raise

@task(dag=dag)
def transform_data(**context):
    """Transform raw weather data into structured format"""
    # Pull data from previous task using XCom
    raw_data = context['ti'].xcom_pull(task_ids='fetch_weather')
    
    if not raw_data:
        raise ValueError("No data received from fetch_weather task")
    
    # Extract and transform weather information
    weather_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'location': 'Kochi',
        'temperature_c': raw_data['temp_C'],
        'temperature_f': raw_data['temp_F'],
        'feels_like_c': raw_data['FeelsLikeC'],
        'humidity': raw_data['humidity'],
        'weather_desc': raw_data['weatherDesc'][0]['value'],
        'wind_speed_kmph': raw_data['windspeedKmph'],
        'pressure': raw_data['pressure'],
        'visibility': raw_data['visibility'],
    }
    
    print(f"Transformed data: Temperature: {weather_data['temperature_c']}°C, Conditions: {weather_data['weather_desc']}")
    
    return weather_data

@task(dag=dag)
def save_to_csv(**context):
    """Save weather data to CSV file"""
    # Pull transformed data from previous task
    weather_data = context['ti'].xcom_pull(task_ids='transform_data')
    
    if not weather_data:
        raise ValueError("No data received from transform_data task")
    
    # Define CSV file path
    csv_path = os.path.join(os.environ.get('AIRFLOW_HOME', '.'), 'weather_data.csv')
    
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(csv_path)
    
    # Write to CSV
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=weather_data.keys())
        
        # Write header only if file doesn't exist
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(weather_data)
    
    print(f"Weather data successfully saved to {csv_path}")

# Define task dependencies using >> operator
fetch_weather() >> transform_data() >> save_to_csv()
