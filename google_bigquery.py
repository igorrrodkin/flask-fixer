from google.cloud import bigquery
from google.oauth2 import service_account
import io
import pandas as pd
import os
from logger import Logger



log = Logger('info.log').logger()


class Loader_to_GBQ:

    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def uploading_csv_to_gbq(self, dataset_id, table_id, filename):
        try:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=[
                        "https://www.googleapis.com/auth/cloud-platform"],
                )
                client = bigquery.Client(credentials=credentials,
                                         project=credentials.project_id)
                log.info('Credentials are valid')
            except FileNotFoundError as err:
                error = f"{type(err).__name__} was raised: {err}"
                return log.warning(error)
            try:
                client.create_dataset(dataset_id)
                dataset_ref = client.dataset(dataset_id)
                log.info('dataset was created')
            except:
                log.warning('dataset with this name was created before')
                dataset_ref = client.dataset(dataset_id)
            try:
                client.create_table(
                    f"{self.project_id}.{dataset_id}.{table_id}")
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} created in {dataset_id}')
            except Exception as error:
                table_ref = dataset_ref.table(table_id)
                log.warning(f'table {table_id} was created before')
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.autodetect = True
            if os.path.isfile(filename) == True:
                with open(filename, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file, table_ref, job_config=job_config)
                print(job.result())
            else:
                return log.warning('Filename is not valid , data was not loaded')

        except:
            return log.error('Something went wrong')

    def uploading_json_to_gbq(self, dataset_id, table_id, json_file, table_schema):
        try:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=[
                        "https://www.googleapis.com/auth/cloud-platform"],
                )
                client = bigquery.Client(credentials=credentials,
                                         project=credentials.project_id)
            except FileNotFoundError as err:
                error = f"{type(err).__name__} was raised: {err}"
                return log.warning('Credentials are not valid' , error)
            try:
                client.create_dataset(dataset_id)
                dataset_ref = client.dataset(dataset_id)
                log.info('dataset was created')
            except:
                dataset_ref = client.dataset(dataset_id)
                log.warning('dataset with this name was created before')
            try:
                client.create_table(
                    f"{self.project_id}.{dataset_id}.{table_id}")
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} created in {dataset_id}')
            except:
                table_ref = dataset_ref.table(table_id)
                log.warning(f'table {table_id} was created before')
            try:
                formatted_schema = []
                for row in table_schema:
                    formatted_schema.append(bigquery.SchemaField(
                        row['name'], row['type'], row['mode']))
            except TypeError:
                return log.warning('schema is not correct')

            if os.path.isfile(json_file) == True:
                df = pd.read_json(json_file)
                json_df = df.to_json(orient='records', lines=True)

                stringio_data = io.StringIO(json_df)

                dataset_ref = client.dataset(dataset_id)
                table_ref = dataset_ref.table(table_id)

                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job_config.schema = formatted_schema
                job = client.load_table_from_file(
                    stringio_data, table_ref, job_config=job_config)
                print(job.result())
                return log.info('Success!')
            else:
                return log.warning('file does not exist')
        except:
            return log.error('Something went wrong')


class Extractor_from_GBQ:
    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def get_data_from_gbq(self, dataset_id, table_id, output_file):
        try:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=[
                        "https://www.googleapis.com/auth/cloud-platform"],
                )

                client = bigquery.Client(credentials=credentials,
                                        project=credentials.project_id)
            except FileNotFoundError as err:
                error = f"{type(err).__name__} was raised: {err}"
                return log.warning('Credentials are not valid' , error)
            try:
                table = bigquery.TableReference.from_string(
                    f"{self.project_id}.{dataset_id}.{table_id}"
                )
                rows = client.list_rows(
                    table
                )
                if (output_file.split(' ')[-1]) != 'csv':
                    return log.warning('output file isnot csv')
                dataframe = rows.to_dataframe(
                    create_bqstorage_client=True,
                )
                dataframe.to_csv(output_file)
                print(dataframe)
            except:
                return log.warning('invalid table_id or dataset_id')
        except:
            return log.warning('Something occured')


# uploader = Extractor_from_GBQ(
#     'thinking-window-332916-3c1ca2526a5a.json', 'thinking-window-332916')
# uploader.get_data_from_gbq(
#     'rfirjnbji', 'efkvmifkvrmi202929292929', 'extracted2.json')
