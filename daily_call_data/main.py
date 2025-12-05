import os
import boto3
import requests
import json
from dotenv import load_dotenv
from datetime import datetime

# S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
# S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
# FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
FINNHUB_TOKEN = "d40ebm1r01qqo3qhj6o0d40ebm1r01qqo3qhj6og"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
ENDPOINT = "http://minio:9000"


s3 = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY, endpoint_url=ENDPOINT)
symbol = "AAPL"
url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_TOKEN}"

try:
    r = requests.get(url)
    data = r.json()
    if "error" in data:
        print(data)
    else:
        str_data = json.dumps(data)
        datetime_path = datetime.now().strftime("%Y/%m/%d")
        datetime_unix = datetime.now().timestamp()
        try:
            s3.put_object(Bucket="bronze", Key=f"quote/{datetime_path}/{datetime_unix}.json", Body=str_data)
        except Exception as e:
            print(f"Error S3: {e}")
except Exception as e:
    print(e)
