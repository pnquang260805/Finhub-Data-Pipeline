import os
import boto3
import requests
import json
import pandas as pd

from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
ENDPOINT = "http://minio:9000"

if __name__ == "__main__":
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_TOKEN}"
    s3 = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY, endpoint_url=ENDPOINT)
    try:
        r = requests.get(url)
        data = r.json()
        buffer = BytesIO()
        if "error" in data:
            print(data)
        else:
            df = pd.DataFrame(data)
            df.to_parquet(buffer, index=True)
            year = datetime.now().strftime("%Y")
            try:
                s3.upload_fileobj(Bucket="bronze", Key=f"symbol/{year}/symbol-{year}.parquet", Fileobj=buffer)
            except Exception as e:
                print(f"Error S3: {e}")
    except Exception as e:
        print(e)