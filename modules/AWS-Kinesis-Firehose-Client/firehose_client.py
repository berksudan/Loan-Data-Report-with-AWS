import csv
import json
import os
import sys
import zipfile
from pathlib import Path

import boto3
import wget

PROPERTIES_FILE = 'properties.json'
CREDENTIALS_FILE = 'credentials.json'


def list_to_csv_line(row) -> str: return ','.join(row) + '\n'


def create_aws_session(aws_access_key_id, aws_secret_access_key, aws_session_token, region, client_service_name):
    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            aws_session_token=aws_session_token,
                            region_name=region)
    return session.client(client_service_name)


def extract_num_lines(file_path: str) -> int:
    return sum(1 for _ in open(file_path))


def download_data(data_url: str, data_parent_dir: str) -> str:
    data_name = data_url.split('/')[-1]
    csv_target_path = Path(data_parent_dir, data_name)
    if not os.path.isfile(csv_target_path):
        if not os.path.exists(data_parent_dir):
            os.makedirs(data_parent_dir)
        print("[INFO] Data is downloading to:", csv_target_path)
        wget.download(data_url, str(csv_target_path))
        print("[INFO] Data is downloaded to:", csv_target_path)
    else:
        print("[INFO] Data exists: " + str(csv_target_path) + ", so need to download.")
    return csv_target_path


def main():
    props = json.load(open(PROPERTIES_FILE))
    credentials = json.load(open(CREDENTIALS_FILE))

    zip_full_path = download_data(data_url=props['DATA_URL'], data_parent_dir=props['LOCAL_CSV_PARENT_DIR'])

    loan_data_loader_firehose_client = create_aws_session(
        aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=credentials['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=credentials['AWS_SESSION_TOKEN'],
        region=props['REGION_NAME'],
        client_service_name='firehose')

    records = []
    with zipfile.ZipFile(zip_full_path, 'r') as zip_ref:
        zip_ref.extractall(props['LOCAL_CSV_PARENT_DIR'])
    csv_full_path = str(Path(props['LOCAL_CSV_PARENT_DIR'], props['LOCAL_CSV_FILE_NAME']))
    num_lines = extract_num_lines(csv_full_path)
    with open(csv_full_path, 'r') as csv_file:
        loan_data_reader = csv.reader(csv_file)

        record_count = 1
        for loan_row in loan_data_reader:
            if record_count % 500 == 0:
                loan_data_loader_firehose_client.put_record_batch(
                    DeliveryStreamName=props['FIREHOSE_DELIVERY_STREAM_NAME'],
                    Records=records)
                print('[INFO] Delta-Transferred:%d, Percentage:%%%.3f' % (record_count, 100 * record_count / num_lines))
                records.clear()
            records.append({"Data": list_to_csv_line(loan_row)})
            record_count += 1
        if len(records) > 0:
            print(len(records))
            response = loan_data_loader_firehose_client.put_record_batch(
                DeliveryStreamName=props['FIREHOSE_DELIVERY_STREAM_NAME'],
                Records=records
            )
            print(response)


if __name__ == '__main__':
    main()
