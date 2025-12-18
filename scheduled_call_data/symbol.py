import os
import json
import pyarrow as pa
import io

from datetime import datetime
from dotenv import load_dotenv

from service.etl_symbol import ETLSymbol
from service.s3_service import S3Service
from common.nessie_catalog import NessieCatalog

load_dotenv()

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
ENDPOINT = "http://minio:9000"

if __name__ == "__main__":
    year = datetime.now().strftime("%Y")
    table_name = "dim_symbol"
    catalog_name =  "iceberg"
    namespace = "dbo"
    table_name = "dim_symbol"

    s3 = S3Service(S3_ACCESS_KEY, S3_SECRET_KEY)
    etl_symbol = ETLSymbol(FINNHUB_TOKEN, s3)
    nessie_catalog = NessieCatalog()
    # Init catalog
    nessie_catalog.init_catalog()
    catalog = nessie_catalog.get_catalog()

    raw_data = etl_symbol.fetch_data()
       
    raw_data_str = json.dumps(raw_data)
    data_stream = io.BytesIO(raw_data_str.encode('utf-8'))
    s3.write_object("files", f"symbol/{year}/symbol-{year}.json", data_stream)

    cleaned_data = etl_symbol.clean_data(raw_data)
    arrow_table = pa.Table.from_pandas(cleaned_data)

    if nessie_catalog.check_namespace_exists("dbo") is False:
        nessie_catalog.create_namespace("dbo")
    try:
        table = catalog.load_table(f"{namespace}.{table_name}")
    except:
        table = catalog.create_table(f"{namespace}.{table_name}", arrow_table.schema)
    
    table.append(arrow_table)
