import pytz
import json
from pytz import timezone
import ftplib
from airflow.models import DAG
from google.cloud import bigquery
from google.cloud import storage
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor
)
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryGetDatasetOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDatasetTablesOperator
)

from airflow.models import Variable


def _push_reco_file(ti):
    ist_time = datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S").replace(' ', '_')
    ist_date = datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
    reco_file_uri = "gs://ndp-analytics-recosense-raw-prd/"+ist_date+"/reco_raw_" + ist_time + "_*.csv"
    ti.xcom_push(key='reco_file_uri', value=reco_file_uri)


def upload_file_to_ftp(ti):
    # reco_file_uri = ti.xcom_pull(key='reco_file_uri', task_ids='push_reco_file')
    ftp = ftplib.FTP("ftp-data.recosenselabs.com")
    ftp.login("data", "c4BW6K4VDL")
    ftp.cwd('tatvic')

    prefix = datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d") + "/"
    storage_client = storage.Client()
    blobs = storage_client.get_bucket('ndp-analytics-recosense-raw-prd').list_blobs(prefix=prefix)
    for blob in blobs:
        blob.download_to_filename("reco_temp_file.csv")
        print("file downloaded")

        with open("reco_temp_file.csv", 'rb') as file:
            ftp_file_name = blob.name.replace(prefix, "")
            print(ftp_file_name)

            ftp.storbinary("STOR " + ftp_file_name, file)  # send the file
            file.close()
            print("File {} Uploaded".format(ftp_file_name))

    # listing files from FTP for verification
    files = []
    ftp.dir(files.append)
    for line in files:
        print(line)

    ftp.quit()


with DAG('recosense_pipeline', schedule_interval='0 3 * * *',
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=["recosens"]) as dag:
    run_query = BigQueryExecuteQueryOperator(
        task_id='run_query',
        sql=Variable.get("recosense_sql"),
        use_legacy_sql=False,
        destination_dataset_table=Variable.get("recosense_temp_table"),
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='bq-service-id'
    )

    push_reco_file_name = PythonOperator(
        task_id='push_reco_file_name',
        python_callable=_push_reco_file,
        provide_context=True
    )

    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bq_to_gcs",
        source_project_dataset_table=Variable.get("recosense_temp_table"),
        destination_cloud_storage_uris="{{ti.xcom_pull(key='reco_file_uri' , task_ids='push_reco_file_name') }}",
        field_delimiter="|",
        export_format="CSV",
        print_header=True,
        gcp_conn_id='bq-service-id'
    )

    upload_file = PythonOperator(
        task_id='upload_file',
        python_callable=upload_file_to_ftp,
        provide_context=True
    )

    run_query >> push_reco_file_name >> bq_to_gcs >> upload_file
