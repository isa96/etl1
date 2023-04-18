from datetime import datetime
import hashlib
import requests
import json
from google.cloud import bigquery
from google.oauth2 import service_account

SA_CREDENTIAL_FILE = 'credentials-kelompok-3.json'
url_endpoint = 'https://api-mobilespecs.azharimm.site/v2/brands'

def extract(endpoint):
    response = requests.get(endpoint)
    return response.json()

def transform(raw_data):
    transformed_data = []
    for data in raw_data:
        transformed_data.append({
            'super_key': hashlib.md5(str(data).encode()).hexdigest(),
            'raw_brands': json.dumps(data),
            'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return transformed_data

def load(transformed_data, table_id):
    credential = service_account.Credentials.from_service_account_file(
        SA_CREDENTIAL_FILE
    )

    client = bigquery.Client(
        credentials = credential,
        project = credential.project_id
    )

    client.insert_rows_json(table_id, transformed_data)
    
    print("Berhasil")

def etl():
    extract()
    transform()
    load()

if __name__ == "__main__":
    raw_data = extract(url_endpoint)['data']
    transformed_data = transform(raw_data)

table_id = 'kelompok_3_stg.stg_brands'
load(transformed_data, table_id)
