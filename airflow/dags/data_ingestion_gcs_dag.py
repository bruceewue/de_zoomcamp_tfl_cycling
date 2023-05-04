import os
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


URL_PREFIX = "https://cycling.data.tfl.gov.uk/usage-stats/"
START_IDX = 350


def fetch_file_names(start_index):
    url = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/"
    response = requests.get(url)
    regex = re.compile(r"(\d+)JourneyDataExtract.*?(?=.csv)")
    files = [file.group() for file in regex.finditer(response.text)]
    filtered_files = [
        file for file in files if int(re.findall("\d+", file)[0]) >= start_index
    ]
    return filtered_files


def download_csv_task(**kwargs):
    filtered_files = kwargs["task_instance"].xcom_pull(task_ids="fetch_file_names_task")
    csv_data = []

    for file in filtered_files:
        url = URL_PREFIX + file + ".csv"
        response = requests.get(url)

        with open(file + ".csv", "wb") as f:
            f.write(response.content)
            csv_data.append(f.name)

    return csv_data


def convert_csv_to_parquet_task(**kwargs):
    column_dict = {
        "number": "rental_id",
        "rentalid": "rental_id",
        "bikenumber": "bike_id",
        "bikeid": "bike_id",
        "enddate": "end_datetime",
        "endstationid": "end_station_id",
        "endstationnumber": "end_station_id",
        "endstationname": "end_station_name",
        "endstation": "end_station_name",
        "startdate": "start_datetime",
        "startstationnumber": "start_station_id",
        "startstationid": "start_station_id",
        "startstationname": "start_station_name",
        "startstation": "start_station_name",
    }
    selected_columns = [
        "rental_id",
        "bike_id",
        "start_datetime",
        "start_station_id",
        "start_station_name",
        "end_datetime",
        "end_station_id",
        "end_station_name",
    ]
    csv_files = kwargs["task_instance"].xcom_pull(task_ids="download_csv_task")
    parquet_files = []

    for csv_file in csv_files:
        print("---read csv_file---", csv_file)
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.lower().str.replace(" ", "")
        df.rename(columns=column_dict, inplace=True)
        df = df[selected_columns]
        for col in [
            "rental_id",
            "bike_id",
            "end_station_id",
            "start_station_id",
            "start_station_name",
            "end_station_name",
        ]:
            df[col] = df[col].astype(str)
        df["start_datetime"] = pd.to_datetime(df["start_datetime"])
        df["end_datetime"] = pd.to_datetime(df["end_datetime"])

        parquet_file = csv_file.replace(".csv", ".parquet")
        df.to_parquet(parquet_file)
        parquet_files.append(parquet_file)
        os.remove(csv_file)

    return parquet_files


def local_to_gcs_task(**kwargs):

    parquet_files = kwargs["task_instance"].xcom_pull(
        task_ids="convert_csv_to_parquet_task"
    )
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET)

    for parquet_file in parquet_files:
        blob = bucket.blob("raw/" + os.path.basename(parquet_file))
        blob.upload_from_filename(parquet_file)

    return parquet_files


def remove_local_data(**kwargs):
    files_to_remove = kwargs["task_instance"].xcom_pull(task_ids="local_to_gcs_task")
    for file in files_to_remove:
        print(f"delete file: {file}")
        os.remove(file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "cycling_data_etl",
    default_args=default_args,
    description="Cycling Data ETL",
    schedule_interval="0 0 1 * *",
    catchup=False,
)

fetch_file_names_task = PythonOperator(
    task_id="fetch_file_names_task",
    provide_context=True,
    python_callable=fetch_file_names,
    op_args=[START_IDX],
    dag=dag,
)

download_csv_task = PythonOperator(
    task_id="download_csv_task",
    provide_context=True,
    python_callable=download_csv_task,
    dag=dag,
)

convert_csv_to_parquet_task = PythonOperator(
    task_id="convert_csv_to_parquet_task",
    provide_context=True,
    python_callable=convert_csv_to_parquet_task,
    dag=dag,
)

local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    provide_context=True,
    python_callable=local_to_gcs_task,
    dag=dag,
)

remove_local_data = PythonOperator(
    task_id="remove_local_data",
    provide_context=True,
    python_callable=remove_local_data,
    dag=dag,
)

(
    fetch_file_names_task
    >> download_csv_task
    >> convert_csv_to_parquet_task
    >> local_to_gcs_task
    >> remove_local_data
)
