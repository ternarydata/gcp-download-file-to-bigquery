import os

from google.cloud import bigquery

def load_csv_to_bq(data, context):
        client = bigquery.Client()
        
        dataset_ref = client.dataset(os.environ['DATASET'])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
                bigquery.SchemaField('date', 'DATE'),
                bigquery.SchemaField('county', 'STRING'),
                bigquery.SchemaField('state', 'STRING'),
                bigquery.SchemaField('fips', 'INTEGER'),
                bigquery.SchemaField('cases', 'INTEGER'),
                bigquery.SchemaField('deaths', 'INTEGER'),
                ]
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV

        # get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://' + data['bucket'] + '/' + data['name']

        # load the data into BQ
        load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table(os.environ['TABLE']),
                job_config=job_config)

        load_job.result()  # wait for table load to complete.
        print('Job finished.')