from google.cloud import bigquery
from google.oauth2 import service_account
import io
import pandas as pd
import logging
# from app import app

# handler = logging.FileHandler("test.log")
# formatter = logging.Formatter(
#     '[%(asctime)s] - %(name)s - %(levelname)s - %(message)s')
# handler.setLevel(logging.DEBUG)
# handler.setFormatter(formatter)
# app.logger.addHandler(handler)

class Loader_to_GBQ:
    
    
    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def uploading_csv_to_gbq(self, dataset_id, table_id, filename):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)
        #handler.info('[INFO] credentials are true')
        try:
            client.create_dataset(dataset_id)
        except:
            pass
        try:
            client.create_table(f"{self.project_id}.{dataset_id}.{table_id}")
        except:
            pass

        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(
                source_file, table_ref, job_config=job_config)
        print(job.result())

    def uploading_json_to_gbq(self, dataset_id, table_id, json_file, table_schema):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)
        try:
            client.create_dataset(dataset_id)
        except:
            pass
        try:
            client.create_table(f"{self.project_id}.{dataset_id}.{table_id}")
        except:
            pass

        formatted_schema = []
        for row in table_schema:
            formatted_schema.append(bigquery.SchemaField(
                row['name'], row['type'], row['mode']))

        df = pd.read_json(json_file)
        json_df = df.to_json(orient='records', lines=True)

        stringio_data = io.StringIO(json_df)

        dataset = client.dataset(dataset_id)
        table = dataset.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = formatted_schema
        job = client.load_table_from_file(
            stringio_data, table, job_config=job_config)
        # print(job.result())
        return 'Success!'


class Extractor_from_GBQ:
    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def get_data_from_gbq(self, dataset_id, table_id, output):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)

        # Download a table.
        table = bigquery.TableReference.from_string(
            f"{self.project_id}.{dataset_id}.{table_id}"
        )
        rows = client.list_rows(
            table
        )
        dataframe = rows.to_dataframe(
            create_bqstorage_client=True,
        )
        dataframe.to_csv(output)
        print(dataframe)

    def extract_data_gbq_official(self, dataset_id, table_id, bucket_name):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)

        #destination_uri = "gs://{}/{}".format(bucket_name, "extracted.csv")
        destination_uri = f"{bucket_name}/extracted2.csv"
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)
        table_ref = dataset_ref.table(table_id)

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )
        extract_job.result()

        print(
            "Exported {}:{}.{} to {}".format(
                self.project_id, dataset_id, table_id, bucket_name)
        )


# uploader = Extractor_from_GBQ(
#     'thinking-window-332916-3c1ca2526a5a.json', 'thinking-window-332916')


# #uploader.extract_data_gbq_official('flask2', 'symbols26-11-31', 'extracted')

# schema = [
#     bigquery.SchemaField("currency", "STRING"),
#     bigquery.SchemaField("name", "STRING"),
# ]
# uploader.get_data_from_gbq('flask2', 'symbols26-11-31' , 'abc.csv')
