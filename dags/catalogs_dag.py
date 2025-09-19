import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd
from plugins.io.postgres_catalog_client import sync_table, truncate_table, update_is_active_field
from plugins.io.postgres_analytics_client import insert_data


default_args = {
    "owner": "airflow_admin"
}

# Соответствие внешних колонок на наши колонки в таблицах БД
category_accordance = {
    "category_name": "category_name",
    "type": "type",
    "guid": "external_uuid",
}

dish_accordance = {
    "dish_name": "dish_name",
    "guid": "external_uuid",
    "category_uuid": "category",
}

product_accordance = {
    "product_name": "product_name",
    "unit": "unit",
    "guid": "external_uuid",
    "category_uuid": "category",
}

recipe_accordance = {
    "dish_guid": "dish_id",
    "product_guid": "product_id",
    "quantity": "quantity",
}


def get_catalog_data(data_type: str) -> dict:
    """
    data_type - categories, dishes, products, recipes
    """
    endpoint = f"/data/get-{data_type}"
    hook = HttpHook(method="GET", http_conn_id="1C_api")
    logging.info(f"Fetching up-to-date data from 1C API: {endpoint}")
    response = hook.run(endpoint)
    logging.info(f"Статус: {response.status_code}")
    response_data = response.json()
    logging.info(f"Всего: {len(response_data[data_type])} данных")
    return response_data[data_type]


@dag(
    default_args=default_args,
    start_date=datetime(2025, 9, 7),
    schedule_interval="@weekly",
    catchup=False
)
def sync_catalogs_dag():
    @task(execution_timeout=timedelta(seconds=60))
    def extract_up_to_date_catalogs_data():
        catalog_data = {
            "categories": None,
            "dishes": None,
            "products": None,
            "recipes": None,
        }
        
        catalog_data["categories"] =  get_catalog_data("categories")
        catalog_data["dishes"] =  get_catalog_data("dishes")
        catalog_data["products"] =  get_catalog_data("products")
        catalog_data["recipes"] =  get_catalog_data("recipes")
    
        return catalog_data
        

    @task(execution_timeout=timedelta(seconds=60))
    def prepare_data(catalog_data: dict) -> dict:
        print("Подготовка данных")
        # TODO: по хорошему соответствие accordance должно быть тут
        logging.info(catalog_data)
        return catalog_data
    
    @task()
    def update_catalog_tables(catalog_data: dict) -> None:
        logging.info("Start updating")
        sync_table("category", catalog_data["categories"], category_accordance, "external_uuid")
        logging.info("Category table has been succesfully synchronized")
        sync_table("dish", catalog_data["dishes"], dish_accordance, "external_uuid", category_fk=True)
        update_is_active_field(catalog_data["dishes"], dish_accordance)
        logging.info("Dish table has been succesfully synchronized")
        sync_table("product", catalog_data["products"], product_accordance, "external_uuid", category_fk=True)
        ### TODO: подразумеваю, что у продуктов не будут удаляться записи, поэтому не использую поле is_active
        logging.info("Product table has been succesfully synchronized")

        truncate_table("recipe")
        recipe_df = pd.DataFrame(catalog_data["recipes"])
        recipe_df.rename(columns=recipe_accordance, inplace=True)
        insert_data(
            data_df=recipe_df, 
            table_name="recipe", 
            fk_on=["dish", "product"]
        )
        logging.info("Resipes table has been succesfully synchronized")
        logging.info("End updating")


    new_catalog_data = extract_up_to_date_catalogs_data()
    prepared_catalog = prepare_data(new_catalog_data)
    upd = update_catalog_tables(prepared_catalog)

    new_catalog_data >> prepared_catalog >> upd


catalogs_dag = sync_catalogs_dag() 