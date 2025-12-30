import os
import boto3
import requests
import json
import pyarrow as pa

from dotenv import load_dotenv
from datetime import datetime

from service.etl_quote import ETLQuote
from service.s3_service import S3Service
from common.nessie_catalog import NessieCatalog

load_dotenv()

S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
ENDPOINT = "http://minio:9000"

if __name__ == "__main__":
    nessie_catalog = NessieCatalog()
    nessie_catalog.init_catalog()
    catalog = nessie_catalog.get_catalog()

    date_ = datetime.now().strftime("%Y/%m/%d")
    catalog_name =  "iceberg"
    namespace = "dbo"
    table_name = "quote"

    symbols = ["AAPL", "AMZN"]
    s3 = S3Service(S3_ACCESS_KEY, S3_SECRET_KEY)

    quote_service = ETLQuote(symbols, FINNHUB_TOKEN)
    raw_data = quote_service.fetch_data()
    s3.write_object("files", f"quote/{date_}/quote.json", json.dumps(raw_data))

    cleaned_data = quote_service.clean_data(raw_data)
    arrow_table = pa.Table.from_pandas(cleaned_data)

    try:
        table = catalog.load_table(f"{namespace}.{table_name}")
        table.append(arrow_table)
    except Exception as e:
        print(f"Error when loading quote with {e}")
    
