import psycopg2
# from config import host, user, password, db_name
from app import log
import json
import requests

class Postgres_connector():

    # def __init__(self, hst, usr, psswrd, db_nm):
    #     self.hst = hst,
    #     self.usr = usr,
    #     self.psswrd = psswrd,
    #     self.db_nm = db_nm

    def extracting_data_from_postgres(self, hst, usr, psswrd, db_nm, table_name, file_path, delimiter):
        try:
            connection = psycopg2.connect(
                host=hst,
                user=usr,
                password=psswrd,
                database=db_nm
            )
            log.info('Succesfully connected to Postgres')
        except:
            return log.warning('Credentials are not valid')
        connection.autocommit = True

        with connection.cursor() as cursor:
            #     cursor.execute(
            #         f"""SELECT relname FROM pg_class WHERE relkind='r'
            #       AND relname !~ '^(pg_|sql_)';"""
            #     )
            #     tables = [i[0] for i in cursor.fetchall()]
            # return tables
            cursor.execute(
                f"""COPY {table_name} TO '{file_path}' WITH DELIMITER '{delimiter}' CSV HEADER;"""
            )
        log.info(f' {table_name} succesfully copied to {file_path}')
        # except:
        #     return log.warning('Table name does not exist')

    def uploading_data(self, hst, usr, psswrd, db_nm, table_name, table_name_temp, path_to_file, on_conflict_column, **columns_types):
        try:
            try:
                connection = psycopg2.connect(
                    host=hst,
                    user=usr,
                    password=psswrd,
                    database=db_nm
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
                    log.info(
                        'Information from temporary table is succesfully copied into table')

                    cursor.execute(
                        f"""
                        DROP TABLE {table_name_temp}
                        """
                    )
                    log.info(f'Temporary table  {table_name_temp} is dropped')
            except psycopg2.errors.SyntaxError as err:
                error = f"{type(err).__name__} was raised: {err}"
                return log.warning(f'data was not uploaded to {table_name}', error)
        except:
            return log.error('Something went wrong')


# postgr = Postgres_connector()
# postgr.uploading_data("127.0.0.1", "ihor", "123456789", "management_api",
#                                      "accounts", "/home/igor/Рабочий стол/flask-fixer2/from_postgres.csv", "|")


# def connect(host, name, password, db_name):
#     psycopg2.connect(
#         host=host,
#         user=name,
#         password=password,
#         database=db_name
#     )
#     return 'all is ok'


# print(connect('127.0.0.1' , 'ihor' , '123456789' , 'management_api'))

# def uploading_data(hst, usr, psswrd, db_nm, table_name, table_name_temp, path_to_file):
#     # try:
#     connection = psycopg2.connect(
#         host=hst,
#         user=usr,
#         password=psswrd,
#         database=db_nm
#     )
    # log.info('Succesfully connected to Postgres')
    # table_structure = ''
    # columns = ''
    # excluded = ''
    # for key, value in columns_types.items():
    #     table_structure += f'{key}  {value} |\n'
    #     columns += f'{key} '
    #     excluded += f'{key} = EXCLUDED.{key} |\n'
    # columns = ','.join(columns.split(' ')[:-1])
    # excluded = ','.join(excluded.split('|')[:-1])
    # table_structure = ','.join(table_structure.split('|')[:-1])
    # if columns == '':
    #     return log.warning('Table characteristics arenot given')
    # connection.autocommit = True
    # # try:
    # with connection.cursor() as cursor:
    #     cursor.execute(
    #         f"""CREATE TEMPORARY TABLE {table_name_temp}(
    #             currency VARCHAR NOT NULL,
    #             name VARCHAR NOT NULL
    #             )"""
    #     )
    # with connection.cursor() as cursor:
    #     # cursor.execute(
    #     #     f"""COPY {table_name_temp}(currency , name)
    #     #         FROM '{path_to_file}'
    #     #         DELIMITER '|'
    #     #         CSV HEADER"""
    #     # )
    #     cursor.execute(
    #         f"""CREATE TABLE IF NOT EXISTS {table_name}(
    #                     currency VARCHAR NOT NULL,
    #                     name VARCHAR NOT NULL
    #                     )"""
    #     )
    # print('created')
    # cursor.execute(
    #     f"""INSERT INTO {table_name}
    #     SELECT * FROM {table_name_temp}

    #     """
    # )
    # log.info(
    #     'Information from temporary table is succesfully copied into table')

    # cursor.execute(
    #     f"""
    #     DROP TABLE {table_name_temp}
    #     """
    # )
    # log.info(f'Temporary table  {table_name_temp} is dropped')
    #     except psycopg2.errors.SyntaxError as err:
    #         error = f"{type(err).__name__} was raised: {err}"
    #         return log.warning(f'data was not uploaded to {table_name}', error)
    # except:
    #     return log.error('Something went wrong')


# uploading_data('127.0.0.1' , 'ihor' , '123456789' , 'management_api' , 'testttable' , 'testttable_temp' ,'/home/igor/Рабочий стол/flask-fixer2/symbols.csv')

class Fixer_Io:
    def __init__(self, access_key):
        self.access_key = access_key

    def request_fixerio(self):
        try:
            response = requests.get(
                f'http://data.fixer.io/api/latest?access_key={self.access_key}')
            json_response = json.dumps(
                response.json()["rates"], indent=4, sort_keys=True)
            currencies = json.loads(json_response).keys()
            data = json.loads(json_response).values()
            response = []
            for currency, data in zip(currencies, data):
                response.append({"currency": currency, "price": data})
            log.info('Succefully extracted from Fixer.IO')
            return response

        # except InvalidSchemaError as err:
        #     return InvalidSchemaError.logging(err)
        except UnboundLocalError:
            log.warning('invalid fixer endpoint')
            return 'invalid fixer endpoint'
        except KeyError:
            log.warning('access key isnot valid , bad request')
            return 'access key isnot valid , bad request'
        except:
            log.warning('something has happened')
            return 'something has happened'


def upload_json_to_postgres(host, name, password, db_name, table_name, json_data):
    connection = psycopg2.connect(
        host=host,
        user=name,
        password=password,
        database=db_name
    )
    table_name_temp = table_name+"_temp"
    table_structure = ''
    columns = ''
    excluded = ''
    on_conflict_column = list(json_data[0].keys())[0]
    for key, value in json_data[0].items():
        if (isinstance(value, str)):
            table_structure += f'{key}  TEXT UNIQUE|\n'
        if (isinstance(value, (float, int))):
            table_structure += f'{key}  FLOAT|\n'
        columns += f'{key} |'
        if on_conflict_column != key:
            excluded += f'{key} = EXCLUDED.{key} |\n'
    columns = ','.join(columns.split('|')[:-1])
    excluded = ','.join(excluded.split('|')[:-1])
    table_structure = ','.join(table_structure.split('|')[:-1])

    connection.autocommit = True
    with connection.cursor() as cursor:

        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name}(
                {table_structure}
                )"""
            )

        query_sql = f""" INSERT INTO {table_name}
                SELECT * FROM json_populate_recordset(NULL::{table_name}, %s)
                ON CONFLICT ({on_conflict_column}) DO UPDATE SET
                {excluded} """

        cursor.execute(
            query_sql, (json.dumps(json_data),)
        )
        log.info('Information from temporary table is succesfully copied into table')

        log.info(f'Temporary table  {table_name_temp} is dropped')
        cursor.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")
        for table in cursor.fetchall():
            print(table)

        postgreSQL_select_Query = f"SELECT * FROM {table_name}"

        cursor.execute(postgreSQL_select_Query)
        # print("Selecting rows from mobile table using cursor.fetchall")
        mobile_records = cursor.fetchall()

        print("Print each row and it's columns values")
        for row in mobile_records:
            type(row[1])
            print("\nName = ", row[0], )
            print("Price = ", row[1])
    print('uploaded')


upload_json_to_postgres('127.0.0.1', 'ihor', '123456789',
                        'management_api', 'asdf3' , Fixer_Io('d3e4904f14d071582e77c128912527c0').request_fixerio())
