import requests
import json
from cryptography.fernet import Fernet
from b64uuid import B64UUID
import re
import gzip
import pickle
import boto3
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
load_dotenv()

api_url = 'http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log'
api_key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='

# 파싱된 데이터 복호화
def decrypt(key, data):
    fernet = Fernet(key)
    for i in range(len(data)):
        temp = fernet.decrypt(data[i]['data']).decode('utf-8').replace("'", "\"")
        data[i]['data'] = json.loads(temp)
    return data

# 복호화된 데이터 문자열 압축
def zip_str(data):
    for i in range(len(data)):
        user_id = data[i]['data']['user_id']
        short_id = B64UUID(user_id[:32]).string + B64UUID(user_id[32:]).string
        data[i]['data']['user_id'] = short_id

        method = data[i]['data']['method']
        if method == 'GET':
            data[i]['data']['method'] = 1
        elif method == 'POST':
            data[i]['data']['method'] = 2
        elif method == 'PUT':
            data[i]['data']['method'] = 3
        else:
            data[i]['data']['method'] = 4

        url = data[i]['data']['url']
        if url == '/api/products/product/':
            data[i]['data']['url'] = 1
        else:
            data[i]['data']['url'] = 0

        indate = data[i]['data']['inDate']
        data[i]['data']['inDate'] = re.sub("[^0-9]","",indate[2:])

    return data

# gzip으로 데이터 압축/저장
def gzip_data(data):
    with gzip.open('./CP1/cp1.gz', 'wb') as f:
        pickle.dump(data, f)

# aws s3 연결
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')

def s3_connection():
    try:
        s3 = boto3.client(
            service_name="s3",
            region_name="ap-northeast-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    except Exception as e:
        print(e)
    else:
        print("s3 bucket connected!") 
        return s3
    

def ETL_pipeline():
    page = requests.get(api_url)
    parsed_data = json.loads(page.text)

    decrypt_data = decrypt(api_key, parsed_data)
    zip_data = zip_str(decrypt_data)
    gzip_data(zip_data)

    s3 = s3_connection()
    s3.upload_file('./CP1/cp1.gz', aws_s3_bucket_name, 'cp1.gz')

ETL_pipeline()

# APScheduler

# if __name__ == '__main__':
#     scheduler = BlockingScheduler()
#     scheduler.add_job(ETL_pipeline, 'interval', seconds = 300)
#     scheduler.start()