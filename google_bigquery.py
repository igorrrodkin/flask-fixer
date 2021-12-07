from google.cloud import bigquery
from google.oauth2 import service_account
from errors import IncorrectFileError, IncorrectFileExtensionError
import google
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
            credentials = service_account.Credentials.from_service_account_file(
                self.key_path, scopes=[
                    "https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(credentials=credentials,
                                     project=credentials.project_id)
            log.info('Credentials are valid')

            try:
                client.create_dataset(dataset_id)
                dataset_ref = client.dataset(dataset_id)
                log.info(f'dataset {dataset_id} was created')
            except google.api_core.exceptions.Conflict:
                dataset_ref = client.dataset(dataset_id)
                log.info(
                    f'dataset {dataset_id} has already been created before')
            try:
                client.create_table(
                    f"{self.project_id}.{dataset_id}.{table_id}")
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} has just been created')
            except google.api_core.exceptions.Conflict:
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} was created before')
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.autodetect = True
            if os.path.isfile(filename) == True:
                with open(filename, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file, table_ref, job_config=job_config)
                    print(job.result().total_rows())
                    log.info(
                        f'DATA WAS LOADED FROM {filename} TO THE TABLE :{table_id}')
            else:
                raise IncorrectFileError
        except FileNotFoundError:
            return log.warning('File with credentials does not exist or filename is not valid')
        except IncorrectFileError:
            return log.warning(f'Incorrect csv file {filename} given in the function argument')
        except:
            return log.warning('Something went wrong')

    def uploading_json_to_gbq(self, dataset_id, table_id, json_file, table_schema):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.key_path, scopes=[
                    "https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(credentials=credentials,
                                     project=credentials.project_id)
            log.info('Credentials are valid')
            try:
                client.create_dataset(dataset_id)
                dataset_ref = client.dataset(dataset_id)
                log.info(f'dataset {dataset_id} was created')
            except google.api_core.exceptions.Conflict:
                dataset_ref = client.dataset(dataset_id)
                log.info(
                    f'dataset {dataset_id} has already been created before , data will be stored in it')
            try:
                client.create_table(
                    f"{self.project_id}.{dataset_id}.{table_id}")
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} has just been created')
            except google.api_core.exceptions.Conflict:
                table_ref = dataset_ref.table(table_id)
                log.info(f'table {table_id} was created before')
            formatted_schema = []
            for row in table_schema:
                formatted_schema.append(bigquery.SchemaField(
                    row['name'], row['type'], row['mode']))

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
                return log.info('Data was succesfully loaded into google bigQuery!')
            else:
                raise IncorrectFileError
        except FileNotFoundError:
            return log.warning(f'File with credentials {self.key_path} does not exist or filename is not valid')
        except IncorrectFileError:
            return log.warning(f'Incorrect json file {json_file} given in the function argument')
        except TypeError:
            return log.warning('TypeError occured. Schema is not correct')
        except:
            return log.warning('Something went wrong')


class Extractor_from_GBQ:
    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def get_data_from_gbq(self, dataset_id, table_id, output_file):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.key_path, scopes=[
                    "https://www.googleapis.com/auth/cloud-platform"],
            )

            client = bigquery.Client(credentials=credentials,
                                     project=credentials.project_id)
            log.info('Credentials are valid')
            table = bigquery.TableReference.from_string(
                f"{self.project_id}.{dataset_id}.{table_id}"
            )
            rows = client.list_rows(
                table
            )
            if (output_file.split('.')[-1]) != 'csv':
                raise IncorrectFileExtensionError
            dataframe = rows.to_dataframe(
                create_bqstorage_client=True,
            )
            dataframe.to_csv(output_file)
        except FileNotFoundError:
            return log.warning(f'File {self.key_path} with credentials does not exist or filename is not valid')
        except IncorrectFileExtensionError:
            return log.warning(f'Output file {output_file} is not CSV , data hasnot been loaded')
        except:
            return log.warning('Something occured')

