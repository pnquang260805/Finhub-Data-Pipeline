import os
import pyarrow as pa

from typing import *
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog, Catalog
from dataclasses import dataclass

load_dotenv()

@dataclass
class CatalogService:
    catalog : Catalog

    def create_namespace(self, namespace_name : str):
        if (namespace_name, ) not in self.catalog.list_namespaces():
            self.catalog.create_namespace(namespace_name)
            print(f"{namespace_name} has been created")
        else:
            print(f"{namespace_name} existed")

    def create_stock_silver_table(self, stock_table_name : str = "stock_silver", namespace_name : str = "dbo"):
        table_identifier  = ".".join([namespace_name, stock_table_name])
        properties={
        "format-version": "2",               # 'format-version'='2'
        "write.format.default": "parquet"    # 'write.format.default'='parquet'
    }
        schema = pa.schema([
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("price", pa.decimal128(10, 2), nullable=True),
            pa.field("volume", pa.int64(), nullable=True),
            pa.field("trade_type", pa.string(), nullable=True),
            pa.field("unix_ts", pa.int64(), nullable=True),
        ])
        try:
            self.catalog.create_table(table_identifier, schema, properties=properties)
        except Exception as e:
            print(f"Error when create stock silver table with {e}")
            raise(e)
        
    def create_dim_symbol_gold(self, table_name : str = "dim_symbol", namespace_name : str = "dbo"):
        table_identifier  = ".".join([namespace_name, table_name])
        stock_schema = pa.schema([
            pa.field("currency", pa.string(), nullable=True),
            pa.field("description", pa.string(), nullable=True),
            pa.field("display_symbol", pa.string(), nullable=True),
            pa.field("figi", pa.string(), nullable=True),
            pa.field("mic", pa.string(), nullable=True),
            pa.field("share_class_figi", pa.string(), nullable=True),
            pa.field("symbol", pa.string(), nullable=True), 
            pa.field("symbol_2", pa.string(), nullable=True),
            pa.field("type", pa.string(), nullable=True),
            pa.field("fetch_date", pa.string(), nullable=True)
        ])
        try:
            self.catalog.create_table(table_identifier, schema=stock_schema)
        except Exception as e:
            print(f"Error when create dim_symbol with {e}")
            raise(e)


class main():
    ENDPOINT = "http://minio:9000"
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

    warehouse_path = "s3://tables/"
    nessie_uri = "http://catalog:19120/iceberg/" 
    catalog_name = "iceberg"
    namespace_name = "dbo"

    props = {             
        "uri": nessie_uri,
    }
    try:
        catalog = load_catalog(catalog_name, **props)
        catalog_service = CatalogService(catalog)

        catalog_service.create_namespace(namespace_name)
        catalog_service.create_dim_symbol_gold()
        catalog_service.create_stock_silver_table()
    except Exception as e:
        print(f"Error {e} ===============================================")
        raise(e)

if __name__ == "__main__":
    main()