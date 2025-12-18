import pandas as pd
import requests
import re

from datetime import datetime

from service.s3_service import S3Service

class ETLSymbol:
    def __init__(self, finnhub_token : str, s3_service : S3Service):
        self.url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={finnhub_token}"
        self.s3_service = s3_service

    def fetch_data(self) -> dict:
        try:
            symbol_req = requests.get(self.url)
            if symbol_req.status_code != 200:
                raise Exception(f"Status code is: {symbol_req.status_code}")
            
            return symbol_req.json()
        except Exception as e:
            print(e)
            raise(e)
        
    def __standardize_columns(self, column_name : str) -> str:
        pattern = r"(?<!^)(?=[A-Z0-9])"
        return re.sub(pattern, "_", column_name).lower()

    def clean_data(self, data : dict) -> pd.DataFrame:
        df = pd.DataFrame(data)
        deduplicated_df = df.drop_duplicates()
        cleaned_df = deduplicated_df.drop("isin", axis=1)
        new_col_names = {name: self.__standardize_columns(name) for name in df.columns}
        new_col_names["shareClassFIGI"] = "share_class_figi"
        renamed_df = cleaned_df.rename(columns=new_col_names)
        renamed_df["fetch_date"] = datetime.now()

        return renamed_df
    
    def write_to_iceberg(self, df : pd.DataFrame, table_name : str, catalog : str) -> None:
        df.to_iceberg(table_name, catalog_name=catalog)

