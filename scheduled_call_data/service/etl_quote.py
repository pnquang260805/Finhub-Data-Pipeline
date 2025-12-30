import pandas as pd
import requests
import re

from typing import List

from service.s3_service import S3Service

class ETLQuote:
    def __init__(self, symbols : List[str], finnhub_token : str):
        # https://finnhub.io/api/v1/quote?symbol=AAPL&token=d4sdv39r01qvsjbh5nl0d4sdv39r01qvsjbh5nlg
        self.symbols = symbols
        self.token = finnhub_token
    
    def fetch_data(self) -> List[dict]:
        result = []
        try:
            for symbol in self.symbols:
                url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={self.token}" 
                quote_req = requests.get(url)
                if quote_req.status_code != 200:
                    raise Exception(f"Status code in quote is: {quote_req.status_code}")
                data = quote_req.json()
                data["symbol"] = symbol
                result.append(data)
            return result
        except Exception as e:
            print(e)
            raise(e)
        
    def clean_data(self, data : List[dict]) -> pd.DataFrame:
        df = pd.DataFrame(data)
        deduplicate = df.drop_duplicates()
        drop_na_df = deduplicate.dropna()
        new_col_names = {
            "c": "close",
            "d": "change",
            "dp": "change_percentage",
            "h": "high",
            "l": "low",
            "o": "open",
            "pc": "previous_close",
            "t": "time"
        }
        renamed_df = drop_na_df.rename(new_col_names, axis=1)
        clean_df = renamed_df
        clean_df['time'] = pd.to_datetime(clean_df['time'], unit='s').astype(str)
        return clean_df