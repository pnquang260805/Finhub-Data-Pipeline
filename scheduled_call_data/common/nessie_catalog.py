import os

from typing import *
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog, Catalog

load_dotenv()


ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

warehouse_path = "s3://tables/"
nessie_uri = "http://catalog:19120/iceberg/main"
catalog_name = "nessie"

class NessieCatalog:
    def __init__(self):
        self.catalog = None

    def init_catalog(self) -> None:
        props = {
            "uri": nessie_uri
        }

        self.catalog = load_catalog(catalog_name, **props)
    
    def get_catalog(self) -> Catalog:
        return self.catalog

    def all_namespace(self) -> List[tuple]:
        return self.catalog.list_namespaces()

    def check_namespace_exists(self, namespace : str) -> bool:
        return (namespace, ) in self.all_namespace()
    
    def create_namespace(self, namespace : str) -> None:
        self.catalog.create_namespace(namespace)
