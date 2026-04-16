import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.sdk import task,dag
import requests
import json 
import pandas as pd

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0,str(AIRFLOW_HOME))


url = "https://opensky-network.org/api/states/all"

default_args = {
    'owner': 'airflow',
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    dag_id = 'flights_ops_medallion_pipe',
    default_args = default_args,
    start_date = datetime(2026,2,20),
    schedule = '*/30 * * * *',
    catchup = False,
)
def flights_ops_medallion_pipe():

    @task.python   
    def run_bronze_ingestion(**context):
        response = requests.get(url,timeout=30)
        response.raise_for_status()
        data = response.json()
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    
        path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")
    
        with open(path, "w") as f:
            json.dump(data, f)
    
        context['ti'].xcom_push(key='bronze_file', value=str(path))

    @task.python
    def run_silver_transform(**context):
        execution_date = context['ds_nodash']
    
        bronze_file = context['ti'].xcom_pull(key='bronze_file', task_ids='run_bronze_ingestion')
        if not bronze_file:
            raise ValueError("No bronze file found in XCom")
    
        silver_path = Path("/opt/airflow/data/silver")
        silver_path.mkdir(parents=True, exist_ok=True)
    
        with open(bronze_file) as f:
            raw = json.load(f)
    
        df_raw = pd.DataFrame(raw['states'])
        df_raw.columns = [
            'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
            'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
            'true_track', 'vertical_rate', 'sensors', 'geo_altitude',
            'squawk', 'spi', 'position_source'
        ]
    
        df = df_raw[['icao24', 'origin_country', 'velocity', 'on_ground']]
    
        output_file = silver_path / f"flights_silver_{execution_date}.csv"
        df.to_csv(output_file, index=False)
    
        print(f"✅ Silver file saved at: {output_file}")
        context['ti'].xcom_push(key='silver_file', value=str(output_file))

       
    @task.python
    def run_gold_aggregate(**context):
        silver_file = context['ti'].xcom_pull(key='silver_file', task_ids='run_silver_transform')
        if not silver_file:
            raise ValueError("No silver file found in XCom")
    
        df = pd.read_csv(silver_file)
    
        agg = df.groupby('origin_country').agg(
            total_flights=('icao24', 'count'),
            avg_velocity=('velocity', 'mean'),
            on_ground=('on_ground', 'sum')
        ).reset_index()
    
        gold_path = Path(silver_file.replace("silver", "gold"))
        context['ti'].xcom_push(key='gold_file', value=str(gold_path))
        agg.to_csv(gold_path, index=False)
    
        print(f"✅ Gold file saved at: {gold_path}")


    
    bronze = run_bronze_ingestion()
    silver = run_silver_transform()
    gold = run_gold_aggregate()

    bronze >> silver >> gold


# instantiating the dag

flights_ops_medallion_pipe()



