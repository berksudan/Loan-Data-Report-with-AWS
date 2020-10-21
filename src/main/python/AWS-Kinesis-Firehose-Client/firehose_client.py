import csv
import os
import sys
import zipfile
from pathlib import Path

import boto3
import wget

DATA_URL = "https://s3-eu-west-1.amazonaws.com/vngrs.com/challenge/loan_report/loan_data.zip"
DELIVERY_STREAM_NAME = "Loan-Data-Loader"
REGION_NAME = "us-east-1"

CSV_PARENT_DIR = "data"
CSV_NAME = "loan.csv"


def list_to_csv_line(row): ','.join(row) + '\n'


def create_aws_session(aws_access_key_id, aws_secret_access_key, aws_session_token, client_service_name='firehose'):
    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            aws_session_token=aws_session_token,
                            region_name=REGION_NAME)
    return session.client(client_service_name)


def extract_num_lines(file_path: str) -> int:
    return sum(1 for _ in open(file_path))


def download_data(data_url:str, data_parent_dir: str) -> str:
    data_name = data_url.split('/')[-1]
    csv_target_path = Path(data_parent_dir,data_name)
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
    # if len(sys.argv) < 4:
    #     exit("[ERROR] Arguments should be <AWS_ACCESS_KEY_ID> <AWS_SECRET_ACCESS_KEY> <AWS_SESSION_TOKEN>")
    zip_full_path = download_data(data_url=DATA_URL, data_parent_dir=CSV_PARENT_DIR)

    loan_data_loader_firehose_client = create_aws_session(
        aws_access_key_id=sys.argv[1],
        aws_secret_access_key=sys.argv[2],
        aws_session_token=sys.argv[3])

    records = []
    print(zip_full_path)
    with zipfile.ZipFile(zip_full_path, 'r') as zip_ref:
        zip_ref.extractall(CSV_PARENT_DIR)
    exit(445)
    num_lines = extract_num_lines(zip_full_path)
    with open(zip_full_path, 'r') as csv_file:
        loan_data_reader = csv.reader(csv_file)

        record_count = 1
        for loan_row in loan_data_reader:
            if record_count % 500 == 0:
                # response = loanDataLoaderFirehoseClient.put_record_batch(
                #     DeliveryStreamName=DELIVERY_STREAM_NAME,
                #     Records=records)
                print('[INFO] Delta-Transferred:%d, Percentage:%%%.3f' % (record_count, 100 * record_count / num_lines))
                records.clear()
            records.append({"Data": list_to_csv_line(loan_row)})
            record_count += 1
        if len(records) > 0:
            print(len(records))
            response = loan_data_loader_firehose_client.put_record_batch(
                DeliveryStreamName=DELIVERY_STREAM_NAME,
                Records=records
            )
            print(response)


if __name__ == '__main__':
    main()
