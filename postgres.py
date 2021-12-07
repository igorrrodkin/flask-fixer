import psycopg2
import os
from dotenv import load_dotenv
from app import log
from errors import IncorrectFileExtensionError ,InvalidSchemaError

load_dotenv()

class Postgres_connector():

    def __init__(self, hst, usr, psswrd, db_nm):
        self.hst = hst,
        self.usr = usr,
        self.psswrd = psswrd,
        self.db_nm = db_nm

    def extracting_data_from_postgres(self,  table_name, file_path, delimiter):
        try:
            connection = psycopg2.connect(
                host=self.hst,
                user=self.usr,
                password=self.psswrd,
                database=self.db_nm
            )
            log.info('Succesfully connected to Postgres')
            if (file_path.split('.')[-1]) != 'csv':
                raise IncorrectFileExtensionError
            connection.autocommit = True
            with open(file_path, "w+"):
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""COPY {table_name} TO '{file_path}' WITH DELIMITER '{delimiter}' CSV HEADER;"""
                    )
                return log.info(f' {table_name} succesfully copied to {file_path}')
        except IncorrectFileExtensionError:
            return log.warning(f'The extension of file {file_path} is not CSV, data hasnot been loaded')
        except psycopg2.errors.UndefinedTable:
            return log.wrning(f'relation {table_name} does not exist. Data has not been loaded')
        except FileNotFoundError:
            return log.warning(f'file path {file_path} is not correct. Data has not been loaded')
        except psycopg2.errors.InsufficientPrivilege:
            return log.warning(f'{file_path} rejected in access.Data has not been loaded')
        except:
            return log.warning('Table name does not exist')

    def uploading_data(self, table_name, table_name_temp, path_to_file, on_conflict_column, **columns_types):
        try:
            connection = psycopg2.connect(
                host=self.hst,
                user=self.usr,
                password=self.psswrd,
                database=self.db_nm
            )
            log.info('Succesfully connected to Postgres')
            if (path_to_file.split('.')[-1]) != 'csv':
                raise IncorrectFileExtensionError

            table_structure = ''
            columns = ''
            excluded = ''

            for key, value in columns_types.items():
                table_structure += f'{key}  {value} |\n'
                columns += f'{key} '
                excluded += f'{key} = EXCLUDED.{key} |\n'
            columns = ','.join(columns.split(' ')[:-1])
            excluded = ','.join(excluded.split('|')[:-1])
            table_structure = ','.join(table_structure.split('|')[:-1])
            if columns == '':
                raise InvalidSchemaError

            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    f"""CREATE TEMPORARY TABLE {table_name_temp}(
                        {table_structure}
                        )"""
                )
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""COPY {table_name_temp}({columns})
                        FROM '{path_to_file}'
                        DELIMITER '|'
                        CSV HEADER"""
                )
                cursor.execute(
                    f"""CREATE TABLE IF NOT EXISTS {table_name}(
                        {table_structure}
                        )"""
                )
                cursor.execute(
                    f"""INSERT INTO {table_name}
                    SELECT * FROM {table_name_temp}
                    ON CONFLICT ({on_conflict_column}) DO UPDATE SET
                    {excluded}
                    """
                )
                cursor.execute(
                    f"""
                    DROP TABLE {table_name_temp}
                    """
                )
                log.info('Data is succesfully loaded into Postgres')
        except psycopg2.errors.SyntaxError:
                return log.warning(f'SyntaxError occured. Data was not uploaded to {table_name}')
        except IncorrectFileExtensionError:
            return log.warning(f'The extension of file {path_to_file} is not CSV, data hasnot been loaded')
        except FileNotFoundError:
            return log.warning(f'file path {path_to_file} is not correct. Data has not been loaded')
        except psycopg2.errors.InsufficientPrivilege:
            return log.warning(f'{path_to_file} rejected in access.Data has not been loaded')
        except InvalidSchemaError:
            return log.warning('Table schema is not in the valid format. Data has not been loaded')
        except:
            return log.warning('Something went wrong')


# user = os.getenv('user')
# host = os.getenv('host')
# password = os.getenv('password')
# db_name = os.getenv('db_name')