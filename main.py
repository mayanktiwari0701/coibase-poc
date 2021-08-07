"""
 upload data from coinbase to AWS s3 in parquet format
"""
from datetime import datetime
import os
import time
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from config import *
from coinbase_auth import CoinbaseWalletAuth


def convert_json_parquet(json_data, out_file_name):
    """Converts json data to parquet file"""
    try:
        df = pd.DataFrame.from_dict(json_data)
        file_name = f'{out_file_name}.parquet'
        df.to_parquet(file_name)
        return file_name
    except Exception as e:
        print(f"Error Converting: Convert json data to parquet; Error is: {str(e)}")
        raise Exception(e)


def initialise_s3_client():
    """Initialises s3 client using boot
    """
    try:
        s3_client = boto3.client('s3')
        return s3_client
    except Exception as e:
        print(f"Error in S3: s3 client initialisation; Error is: {str(e)}")
        raise Exception(e)


def upload_to_aws_s3(s3_client, local_file, bucket, s3_file):
    """Uploads file from ec2 to aws s3
    """
    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError as e:
        print(f"Error file not found: Upload s3 file; The file '{local_file}' was not found")
        raise Exception(e)
    except NoCredentialsError as e:
        print("Error access; Upload s3 file ; Credentials not available")
        raise Exception(e)
    except Exception as e:
        print(f"Error ; Upload s3 file; Error is: {str(e)}")
        raise Exception(e)


def make_request(url, headers={}, method="GET", auth=None, req_data={}):
    """request using python requests. GET and POST option    """
    response = None
    try:
        if method in ["GET", "get"]:
            response = requests.get(headers=headers, url=url, params=req_data, auth=auth)
        elif method in ["POST", "post"]:
            response = requests.post(headers=headers, url=url, json=req_data, auth=auth)
        else:
            print("Method type: This {method} is not supported !")
            return None
        result = response.json().get("data")
        if(type(result.get("data")) == list):
                return result
        result['timestamp'] = datetime.utcnow()
        return [result]
    except Exception as e:
        print(f"Error ; Fetch data from coinbase; Error is: {str(e)}")
        raise Exception


def delete_file(file_path):
    """Deletes file from local ec2 """
    try:
        os.remove(file_path)
    except Exception as e:
        print(f"Error ; Deleting file from local; Error is: {str(e)}")
        raise Exception(e)


def fetch_and_upload(path, filename, method="GET", auth=None, req_data={}):
    """ Fetch  data from coinbase api, convert to parquet file and uploads to s3 """
    parquet_filepath = None
    print(f"\nProcessing started for API path: '{path}' ...")
    try:
        # 1. Fetch Data from Coinbase
        url = f"{COINBASE_API_BASE_URL}{path}"
        print(f"Fetching data from API '{url}'")
        json_data = make_request(url, {}, method, auth, req_data=req_data)
        print(json_data)
        # 2. Convert JSON to Parquet
        print(f"Converting JSON response to parquet")
        parquet_filepath = convert_json_parquet(json_data, filename)
        # # 3. Upload parquet file to S3
        s3_client = initialise_s3_client()
        current_datetime_str = datetime.now().strftime(DATETIME_FILENAME_FMT)
        foldername = filename
        s3_file_path = f"{foldername}/{filename}-{current_datetime_str}.parquet"
        print(f"Uploading file '{parquet_filepath}' to s3")
        upload_to_aws_s3(s3_client, parquet_filepath, AWS_S3_BUCKET, s3_file_path)
        # # 4. Delete file from local
        print(f"Deleting file '{parquet_filepath}' from local")
        delete_file(parquet_filepath)
        print(f"Processing completed for path: '{path}' and file uploaded is '{s3_file_path}'")
    except Exception:
        # if error delete the temp file created
        if parquet_filepath and os.path.isfile(parquet_filepath):
            print(f"Deleting file '{parquet_filepath}' from local")
            delete_file(parquet_filepath)


def execute():

    # auth = CoinbaseWalletAuth(COINBASE_API_KEY, COINBASE_SECRET_KEY)  in case needed for further development
    # 1.  Currencies data present in coinbase
    fetch_and_upload(CURRENCIES_PATH, "currencies")
    while(True):
    # 2.  Price of bitcoin in USD to be loaded per hour
        fetch_and_upload(PRICES_SPOT_BITCOIN_PATH, "bitcoin_price")
        time.sleep(3600)


def get_price_at_time(date):
    # 3.  Price of bitcoin in USD by date
    req_data = {'date': date}
    fetch_and_upload(PRICES_SPOT_BITCOIN_PATH, "bitcoin_price_historical", req_data=date)

if (__name__ == '__main__'):
    print(" uploading coinbase data for currencies and hourly spot")
    execute()
    print("\nCompleted!")
