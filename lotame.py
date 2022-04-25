import pytz
import json
import os
from pytz import timezone
import pysftp
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

ist_date = datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d")


def _push_lotame_file(ti):
    ist_time = datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S").replace(' ', '_')
    lotame_file_uri = "gs://ndp-analytics-lotame-raw-prd/"+ist_date+"/lotame_raw_" + ist_time + "_*.json"
    ti.xcom_push(key='lotame_file_uri', value=lotame_file_uri)
    print("file name pushed")


def upload_file_to_sftp():
    # getting data from the bigquery
    current_date = datetime.now(timezone('Asia/Kolkata')).strftime("%Y%m%d")
    query = "SELECT   userid,  country,  idtype,  ARRAY_AGG(segments IGNORE NULLS) AS segments FROM " \
            "(SELECT userid, country, idtype, [region, logged_in, platform, logged_in_type, gender ] AS segments  " \
            "FROM `api-project-911592150515.raw_ds_prd.lotame-temp-tbl`),  UNNEST(segments) segments " \
            "GROUP BY   1,  2,  3"
    bq_client = bigquery.Client()
    query_job = bq_client.query(query)
    dataframe = query_job.to_dataframe()
    print("Number of rows from BQ: {}".format(dataframe.shape[0]))
    dataframe.to_json("segmentmembership.json", orient="records", lines=True)

    os.system("rm segmentmembership.json.gz")
    os.system("rm segmentmembership.json.gz.md5")
    os.system("gzip segmentmembership.json")
    os.system("md5sum segmentmembership.json.gz > segmentmembership.json.gz.md5")
    os.system("touch " + current_date + ".done")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    privet_key = '/home/airflow/gcs/data/lotame_ssh_key'
    host = 'booth.crwdcntrl.net'
    username = 'sonylivtatvic'

    with pysftp.Connection(host=host, username=username, cnopts=cnopts, port=22, private_key=privet_key) as sftp:
        with sftp.cd('sonylivtatvic_mid_16313'):
            sftp.mkdir(current_date)
            print(sftp.listdir())
            with sftp.cd(current_date):
                sftp.put('segmentmembership.json.gz')
                sftp.put('segmentmembership.json.gz.md5')
                sftp.put(current_date + '.done')
                print(sftp.listdir())

    os.system('gsutil -m mv segmentmembership.json.gz gs://ndp-analytics-lotame-raw-prd/'+ist_date+'/'+ist_date+'_segmentmembership.json.gz')
    os.system('gsutil -m mv segmentmembership.json.gz.md5 gs://ndp-analytics-lotame-raw-prd/' + ist_date + '/' + ist_date + '_segmentmembership.json.gz.md5')
    os.system('gsutil -m mv '+current_date+'.done gs://ndp-analytics-lotame-raw-prd/' + ist_date + '/' + current_date + '.done')


with DAG('lotame_pipeline', schedule_interval='0 4 * * *',
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=["lotame"]) as dag:

    run_query = BigQueryExecuteQueryOperator(
        task_id='run_query',
        sql=Variable.get("lotame_query"),
        use_legacy_sql=False,
        destination_dataset_table=Variable.get("lotame_temp_table"),
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='bq-service-id'
    )

    push_lotame_file = PythonOperator(
        task_id='push_lotame_file',
        python_callable=_push_lotame_file,
        provide_context=True
    )

    upload_file = PythonOperator(
        task_id='upload_file',
        python_callable=upload_file_to_sftp,
        provide_context=True
    )

    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bq_to_gcs",
        source_project_dataset_table=Variable.get("lotame_temp_table"),
        destination_cloud_storage_uris="{{ti.xcom_pull(key='lotame_file_uri', task_ids='push_lotame_file') }}",
        export_format="JSON",
        gcp_conn_id='bq-service-id'
    )

    run_query >> push_lotame_file >> upload_file >> bq_to_gcs
