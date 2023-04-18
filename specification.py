from datetime import datetime
import hashlib
import requests
import json
from google.cloud import bigquery
from google.oauth2 import service_account

SA_CREDENTIAL_FILE  = 'credentials-kelompok-3.json'

url_endpoint_asus_1 = 'https://api-mobilespecs.azharimm.site/v2/asus_rog_phone_5s_pro-11053'
url_endpoint_asus_2 = 'https://api-mobilespecs.azharimm.site/v2/asus_smartphone_for_snapdragon_insiders-11006'
url_endpoint_asus_3 = 'https://api-mobilespecs.azharimm.site/v2/asus_zenfone_8_flip-10892'
url_endpoint_asus_4 = 'https://api-mobilespecs.azharimm.site/v2/asus_zenfone_4_max_zc520kl-8811'
url_endpoint_asus_5 = 'https://api-mobilespecs.azharimm.site/v2/asus_zenfone_4_selfie_zd553kl-8784'

url_endpoint_huawei_1 = 'https://api-mobilespecs.azharimm.site/v2/huawei_matepad_pro_11_(2022)-11720'
url_endpoint_huawei_2 = 'https://api-mobilespecs.azharimm.site/v2/huawei_nova_10_pro-11640'
url_endpoint_huawei_3 = 'https://api-mobilespecs.azharimm.site/v2/huawei_nova_10-11641'
url_endpoint_huawei_4 = 'https://api-mobilespecs.azharimm.site/v2/huawei_nova_y90-11639'
url_endpoint_huawei_5 = 'https://api-mobilespecs.azharimm.site/v2/huawei_matepad_10_4_(2022)-11525'

def extract(endpoint):
    response = requests.get(endpoint)
    return response.json()

def transform(raw_data):
    transformed_data = {
        'super_key': hashlib.md5(str(raw_data).encode()).hexdigest(),
        'raw_spec': json.dumps(raw_data),
        'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
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
    raw_data = extract(url_endpoint_huawei_5)['data']
    transformed_data = [transform(raw_data)]

table_id = 'kelompok_3_stg.stg_spec_phones'
load(transformed_data, table_id)