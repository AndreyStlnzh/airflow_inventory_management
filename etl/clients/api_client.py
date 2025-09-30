import logging
from typing import List
import pandas as pd
import requests


class APIClient:
    # Заглушка для 1С
    def __init__(self, base_url: str):
        self.base_url = base_url

    def _get_data_from_api(self, data_type: str) -> List[dict]:
        endpoint = f"{self.base_url}/data/get-{data_type}"
        logging.info(f"Fetching up-to-date data from 1C API: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        response_data = response.json()
        logging.info(f"Всего: {len(response_data[data_type])} данных")
        return response_data[data_type]

    def get_sales_data(self) -> pd.DataFrame:
        return pd.DataFrame(self._get_data_from_api("sales"))
    
    def get_stock_data(self) -> pd.DataFrame:
        return pd.DataFrame(self._get_data_from_api("virtual-stock"))
    
    def get_categories_data(self) -> List[dict]:
        return self._get_data_from_api("categories")
    
    def get_dishes_data(self) -> List[dict]:
        return self._get_data_from_api("dishes")
    
    def get_products_data(self) -> List[dict]:
        return self._get_data_from_api("products")
    
    def get_recipes_data(self) -> List[dict]:
        return self._get_data_from_api("recipes")
    
