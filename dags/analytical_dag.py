import logging
import pandas as pd

from datetime import datetime, timedelta
from typing import List, Tuple

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook

from plugins.io.postgres_analytics_client import insert_data
from plugins.io.serializers.dataframe import dataframe_to_bytes, bytes_to_dataframe
from plugins.io.s3_client import uploud_to_minio, download_from_minio

default_args = {
    "owner": "airflow_admin"
}

sale_accordance = {
    "date": "date",
    "dish_guid": "dish_id",
    "quantity": "quantity",
}

virtual_stock_accordance = {
    "date": "date",
    "product_guid": "product_id",
    "estimated_qty": "estimated_qty",
}

# TODO: перенести в plugins
def get_operative_data(data_type: str) -> dict:
    """
    data_type - sales, virtual-stock, recipe
    """
    endpoint = f"/data/get-{data_type}"
    hook = HttpHook(method="GET", http_conn_id="1C_api")
    logging.info(f"Fetching up-to-date data from 1C API: {endpoint}")
    response = hook.run(endpoint)
    logging.info(f"Статус: {response.status_code}")
    response_data = response.json()
    logging.info(f"Всего: {len(response_data[data_type])} данных")
    logging.info(response_data[data_type])
    return response_data[data_type]


def extract_and_save_raw_data(
    data_type: str, 
    minio_bucket: str
) -> str:
    """
    Функция извлечения сырых данных с 1С и сохранения в минио

    Args:
        data_type (str): тип извлекаемых данных. [sales, virtual-stock, recipe,]
        minio_bucket (str): название бакета
    Returns:
        str: путь до сырого файла в minio
    """
    logging.info(f"\nExtracting raw data...: {data_type}")
    extracted_data = get_operative_data(data_type)
    extracted_df = pd.DataFrame(extracted_data)
    logging.info("Data extracted from 1C, beginning of serialization")
    extracted_data_bytes = dataframe_to_bytes(dataframe=extracted_df, format="parquet")
    logging.info("Uplouding final data minio")
    minio_path = uploud_to_minio(
        data_bytes=extracted_data_bytes,
        minio_bucket=minio_bucket,
        format="parquet",
        folder=f"raw/{data_type}/"
    )
    logging.info(f"The data is uploaded to minio: {minio_path}")
    return minio_path


def download_df_from_minio(
    filepath: str,
    minio_bucket: str
) -> pd.DataFrame:
    logging.info(f"\nReading from a minio, file: {filepath}")
    raw_data_bytes = download_from_minio(
        minio_path=filepath,
        minio_bucket=minio_bucket,
    )
    logging.info("The data is downloaded from the minio. The beginning of deserialization")
    data_df = bytes_to_dataframe(file_bytes=raw_data_bytes, format="parquet")
    logging.info("The data has been deserialized")
    return data_df


def download_prepare_and_save_data(
    raw_data_path: str,
    accardance_dict: dict,
    minio_bucket: str,
    group_for_aggregation: List[str] | None = None,
    save_folder: str = ""
) -> str:
    data_df = download_df_from_minio(raw_data_path, minio_bucket)
    data_df = data_df.rename(columns=accardance_dict)
    logging.info("The columns were renamed according to accordance")
    if group_for_aggregation:
        data_df = data_df.groupby(group_for_aggregation, as_index=False).sum()
        logging.info("The data is aggregated by days and dishes")
    
    data_bytes = dataframe_to_bytes(dataframe=data_df, format="parquet")
    minio_path = uploud_to_minio(
        data_bytes=data_bytes, 
        minio_bucket=minio_bucket,
        format="parquet",
        folder=f"prepared/{save_folder}/"
    )

    logging.info(f"The data is uploaded to minio: {minio_path}")
    return minio_path


@dag(
    default_args=default_args,
    start_date=datetime(2025, 9, 7),
    schedule_interval="@daily",
    catchup=False
)
def analytical_dag():
    minio_bucket = Variable.get("minio_bucket")

    @task(execution_timeout=timedelta(seconds=60))
    def get_up_to_date_data() -> dict:
        """
        Функция извлечения сырых данных с 1С и сохранения в минио
        Сохраняет сырые данные в формате parquet
        1. sales - продажи
        2. virtual-stock - виртуальный склад
        3. recipes - рецепты блюд
        4. forecast - прогноз продаж (TODO: Для теста, убрать)
        """
        sales_path = extract_and_save_raw_data("sales", minio_bucket)
        virtual_stock_path = extract_and_save_raw_data("virtual-stock", minio_bucket)

        return {
            "sales_minio_path": sales_path,
            "virtual_stock_minio_path": virtual_stock_path,
        }
    
    @task(execution_timeout=timedelta(seconds=300))
    def prepare_data(raw_data_paths: dict) -> dict:
        """
        Функция подготовки данных для аналитики
        1. Скачивает сырые данные из минио
        2. Агрегирует данные по дням. Суммируются данные одного дня и блюда
        3. Сохраняет подготовленные данные в минио
        """
        logging.info("Начало трансформации данных")
        
        sales_path = download_prepare_and_save_data(
            raw_data_path=raw_data_paths["sales_minio_path"],
            accardance_dict=sale_accordance,
            minio_bucket=minio_bucket,
            group_for_aggregation=["date", "dish_id"],
            save_folder="sales"
        )
        virtual_stock_path = download_prepare_and_save_data(
            raw_data_path=raw_data_paths["virtual_stock_minio_path"],
            accardance_dict=virtual_stock_accordance,
            minio_bucket=minio_bucket,
            save_folder="virtual-stock"
        )

        logging.info("The data is prepared")
        return {
            "sales_minio_path": sales_path,
            "virtual_stock_minio_path": virtual_stock_path,
        }
    
    @task(execution_timeout=timedelta(seconds=300))
    def update_analytic_tables(prep_data_paths: dict) -> None:
        logging.info("The beginning of updating analytical tables")
        data_df = download_df_from_minio(prep_data_paths["sales_minio_path"], minio_bucket)
        insert_data(data_df, table_name="sale", fk_on=["dish"])
        logging.info("The sales table has been updated")
        data_df = download_df_from_minio(prep_data_paths["virtual_stock_minio_path"], minio_bucket)
        insert_data(data_df, table_name="virtual_stock", fk_on=["product"])
        logging.info("The virtual stock table has been updated")

        logging.info("Analytical data has been updated")


    @task(execution_timeout=timedelta(seconds=300))
    def make_forecast() -> None:
        endpoint = f"/forecast/run"
        hook = HttpHook(method="POST", http_conn_id="forecast")
        logging.info(f"Fetching up-to-date data from 1C API: {endpoint}")
        response = hook.run(endpoint)
        logging.info(f"Статус: {response.status_code}")
        if response.status_code == 200:
            logging.info("Прогнозирование выполнено успешно")

    
    

    raw_data_paths = get_up_to_date_data()
    prep_data_paths = prepare_data(raw_data_paths)
    upd = update_analytic_tables(prep_data_paths)
    forecast = make_forecast()

    raw_data_paths >> prep_data_paths >> upd >> forecast


analytical_dag_var = analytical_dag() 