import psycopg2
from config import host, user, password, db_name
from app import log



class Postgres_connector():

    def __init__(self, hst , usr,psswrd,db_nm):
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
        except:
            return log.warning('Credentials are not valid')
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""COPY {table_name} TO '{file_path}' WITH DELIMITER '{delimiter}' CSV HEADER;"""
                )
            log.info(f' {table_name} succesfully copied to {file_path}')
        except:
            return log.warning('Table name does not exist')

    def uploading_data(self , table_name, table_name_temp, path_to_file, on_conflict_column, **columns_types):
        try:
            try:
                connection = psycopg2.connect(
                    host=self.hst,
                    user=self.usr,
                    password=self.psswrd,
                    database=self.db_nm
                )
                log.info('Succesfully connected to Postgres')
            except:
                return log.warning('Credentials are not valid')
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
                return log.warning('Table characteristics arenot given')
            connection.autocommit = True
            try:
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
                    log.info('Information from temporary table is succesfully copied into table')

                    cursor.execute(
                        f"""
                        DROP TABLE {table_name_temp}
                        """
                    )
                    log.info(f'Temporary table  {table_name_temp} is dropped')
            except psycopg2.errors.SyntaxError as err:
                error = f"{type(err).__name__} was raised: {err}"
                return log.warning(f'data was not uploaded to {table_name}' , error)
        except:
           return log.error('Something went wrong')


# postgr = Postgres_connector(host, user, password, db_name)
# postgr.uploading_data('table_a', 'table_a_temp',
#                       '/home/igor/Рабочий стол/flask-fixer/symbols.csv', 'currency')
