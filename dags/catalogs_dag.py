import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandas as pd

from etl.clients.api_client import APIClient
from etl.clients.postgres_client import PostgresClient
from plugins.io.api_hook import APIHook
from plugins.io.postgres_hook import PostgresHook


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


@dag(
    default_args=default_args,
    start_date=datetime(2025, 9, 7),
    schedule_interval="@weekly",
    catchup=False
)
def sync_catalogs_dag():
    api_hook = APIHook(conn_id="1C_api")
    api_client: APIClient = api_hook.get_client()

    postgres_hook = PostgresHook("postgres_conn")
    postgres_client: PostgresClient = postgres_hook.get_client()

    @task(execution_timeout=timedelta(seconds=60))
    def extract_up_to_date_catalogs_data() -> dict:
        catalog_data = {
            "categories": None,
            "dishes": None,
            "products": None,
            "recipes": None,
        }
        
        catalog_data["categories"] =  api_client.get_categories_data()
        catalog_data["dishes"] =  api_client.get_dishes_data()
        catalog_data["products"] =  api_client.get_products_data()
        catalog_data["recipes"] =  api_client.get_recipes_data()
    
        return catalog_data
        

    @task(execution_timeout=timedelta(seconds=60))
    def prepare_data(catalog_data: dict) -> dict:
        print("Подготовка данных")
        # TODO: по хорошему соответствие accordance должно быть тут
        logging.info(catalog_data)
        return catalog_data
    
    @task(execution_timeout=timedelta(seconds=60)) # TODO: при тестовых данных
    def update_catalog_tables(catalog_data: dict) -> None:
        logging.info("Start updating")
        postgres_client.sync_table("category", catalog_data["categories"], category_accordance, "external_uuid")
        logging.info("Category table has been succesfully synchronized")
        postgres_client.sync_table("dish", catalog_data["dishes"], dish_accordance, "external_uuid", category_fk=True)
        postgres_client.update_is_active_field(catalog_data["dishes"], dish_accordance)
        logging.info("Dish table has been succesfully synchronized")
        postgres_client.sync_table("product", catalog_data["products"], product_accordance, "external_uuid", category_fk=True)
        ### TODO: подразумеваю, что у продуктов не будут удаляться записи, поэтому не использую поле is_active
        logging.info("Product table has been succesfully synchronized")

        postgres_client.truncate_table("recipe")
        recipe_df = pd.DataFrame(catalog_data["recipes"])
        recipe_df.rename(columns=recipe_accordance, inplace=True)
        postgres_client.insert_analytic_data(
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