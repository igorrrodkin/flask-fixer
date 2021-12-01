from flask import Flask, render_template, request
from fixer_response import Fixer_Io
from google_bigquery import Loader_to_GBQ, Extractor_from_GBQ
import logging
import logging.handlers
from config import access_key
from logger import Logger

app = Flask(__name__)

log = Logger('info.log').logger()


@app.route('/')
def main_pg():
    return render_template('main.html')


@app.route('/fixer')
def fixer():
    return render_template('fixer.html')


@app.route('/fixer', methods=['POST'])
def select_form_fixer():
    try:
        text = request.form['select-endpoint']
        if (text == 'latest'):
            log.info(' Latest currencies  were loaded from fixer.io in JSON format')
            Fixer_Io(access_key).request_fixerio('latest' , 'price'  ,'latestClass.json')
            return render_template('fixer_response.html')
        if (text == 'symbols'):
            log.info(' Symbols  were loaded from fixer.io in JSON format')
            Fixer_Io(access_key).request_fixerio('symbols' , 'name' , 'symbolsClass.json')
            return render_template('fixer_response.html')
    except:
        log.error(' fixer request falied')
        return render_template('error_html.html')


@app.route('/gbq/upload/json')
def upload_gbq():
    return render_template('gbq_upload.html')


@app.route('/gbq/upload/json', methods=['POST'])
def upload_gbq_json_submit():
    try:
        key_file = request.form['key-file']
        project_id = request.form['project-id']
        if (str(request.form['select-data']) == 'latest'):
            filename = 'latest.json'
            table_schema = {
                'name': 'currency',
                'type': 'STRING',
                'mode': 'NULLABLE'
            }, {
                'name': 'price',
                'type': 'STRING',
                'mode': 'NULLABLE'
            }
        if (str(request.form['select-data']) == 'symbols'):
            filename = 'symbols.json'
            table_schema = {
                'name': 'currency',
                'type': 'STRING',
                'mode': 'NULLABLE'
            }, {
                'name': 'name',
                'type': 'STRING',
                'mode': 'NULLABLE'
            }
        dataset = request.form['dataset']
        table = request.form['table']
        uploader = Loader_to_GBQ(key_file, project_id)
        uploader.uploading_json_to_gbq(dataset, table, filename, table_schema)
        log.info(' JSON is uploaded to BigQuery')
        return render_template('gbq_response.html')
    except:
        log.error(' ERROR while uploading JSON into BigQuery')
        return render_template('error_html.html')


@app.route('/gbq/upload/csv')
def upload_gbq_csv():
    return render_template('gbq_upload.html')


@app.route('/gbq/upload/csv', methods=['POST'])
def upload_gbq_csv_submit():
    try:
        key_file = request.form['key-file']
        project_id = request.form['project-id']
        if (str(request.form['select-data']) == 'latest'):
            filename = 'latest.csv'

        if (str(request.form['select-data']) == 'symbols'):
            filename = 'symbols.csv'
        dataset = request.form['dataset']
        table = request.form['table']
        uploader = Loader_to_GBQ(key_file, project_id)
        uploader.uploading_csv_to_gbq(dataset, table, filename)
        log.info(' CSV is uploaded')
        return render_template('gbq_response.html')
    except:
        log.error(' error while uploading CSV into gbq')
        return render_template('error_html.html')


@app.route('/gbq/extract/csv')
def extract_table():
    return render_template('extractor.html')


@app.route('/gbq/extract/csv', methods=['POST'])
def extract_table_submit():
    try:
        key_file = request.form['key-file']
        project_id = request.form['project-id']
        dataset = request.form['dataset']
        table = request.form['table']
        output = request.form['destination']
        uploader = Extractor_from_GBQ(key_file, project_id)
        uploader.get_data_from_gbq(dataset, table, output)
        log.info(' Table is extracted')
        return render_template('gbq_response.html')
    except:
        log.error(' error while extracting CSV from gbq')
        return render_template('error_html.html')


if __name__ == "__main__":
    app.run(debug=True)
