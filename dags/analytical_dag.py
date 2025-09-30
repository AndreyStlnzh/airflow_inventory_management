import logging

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

from etl.clients.api_client import APIClient
from etl.clients.minio_client import MinioClient
from etl.clients.postgres_client import PostgresClient
from etl.transformaions.sales_transform import sales_raw_to_prepared, stock_raw_to_prepared
from plugins.io.api_hook import APIHook
from plugins.io.minio_hook import MinioHook
from plugins.io.postgres_hook import PostgresHook


default_args = {
    "owner": "airflow_admin"
}

sale_accordance = {
    "date": "date",
    "dish_guid": "dish_id",
    "quantity": "quantity",
}

stock_accordance = {
    "date": "date",
    "product_guid": "product_id",
    "estimated_qty": "estimated_qty",
}


@dag(
    default_args=default_args,
    start_date=datetime(2025, 9, 7),
    schedule_interval="@daily",
    catchup=False
)
def analytical_dag():
    api_hook = APIHook(conn_id="1C_api")
    api_client: APIClient = api_hook.get_client()

    minio_bucket = Variable.get("minio_bucket")
    minio_hook = MinioHook(conn_id="minio_conn", bucket_name=minio_bucket)
    minio_client: MinioClient = minio_hook.get_client()

    postgres_hook = PostgresHook("postgres_conn")
    postgres_client: PostgresClient = postgres_hook.get_client()

    @task(execution_timeout=timedelta(seconds=60))
    def get_up_to_date_data() -> dict:
        """
        Функция извлечения сырых данных с 1С и сохранения в минио
        Сохраняет сырые данные в формате parquet
        1. sales - продажи
        2. virtual-stock - виртуальный склад
        """
        sales_data_df = api_client.get_sales_data()
        stock_data_df = api_client.get_stock_data()

        sales_path = minio_client.upload(sales_data_df, "parquet", folder="raw/sales")
        stock_path = minio_client.upload(stock_data_df, "parquet", folder="raw/virtual-stock")

        return {
            "sales_minio_path": sales_path,
            "stock_minio_path": stock_path,
        }
    
    @task(execution_timeout=timedelta(seconds=300))
    def prepare_data(raw_data_paths: dict) -> dict:
        """
        Функция подготовки данных для аналитики
        1. Скачивание сырых данных из минио
        2. Выполнение трансформаций
        3. Сохранение подготовленных данных в минио
        """
        logging.info("The beginning of preparetion")
        # Для продаж
        sales_data_df = minio_client.download(
            filename=raw_data_paths["sales_minio_path"], 
            format="parquet"
        )
        sales_data_df_prep = sales_raw_to_prepared(sales_data_df, sale_accordance)
        sales_path= minio_client.upload(
            data_df=sales_data_df_prep,
            format="parquet",
            folder="prepared/sales"
        )
        # Теперь для склада
        stock_data_df = minio_client.download(
            filename=raw_data_paths["stock_minio_path"], 
            format="parquet"
        )
        stock_data_df_prep = stock_raw_to_prepared(stock_data_df, stock_accordance)
        stock_path= minio_client.upload(
            data_df=stock_data_df_prep,
            format="parquet",
            folder="prepared/virtual-stock"
        )
        logging.info("The end of preparation")
        return {
            "sales_minio_path": sales_path,
            "stock_minio_path": stock_path,
        }
    
    @task(execution_timeout=timedelta(seconds=300))
    def update_analytic_tables(prep_data_paths: dict) -> None:
        logging.info("The beginning of updating analytical tables")
        
        data_df = minio_client.download(prep_data_paths["sales_minio_path"], "parquet")
        postgres_client.insert_analytic_data(data_df, table_name="sale", fk_on=["dish"])
        logging.info("The sales table has been updated")
        data_df = minio_client.download(prep_data_paths["stock_minio_path"], "parquet")
        postgres_client.insert_analytic_data(data_df, table_name="virtual_stock", fk_on=["product"])
        logging.info("The virtual stock table has been updated")

        logging.info("Analytical data has been updated")


    # @task(execution_timeout=timedelta(seconds=300))
    # def make_forecast() -> None:
    #     endpoint = f"/forecast/run"
    #     hook = HttpHook(method="POST", http_conn_id="forecast")
    #     logging.info(f"Fetching up-to-date data from 1C API: {endpoint}")
    #     response = hook.run(endpoint)
    #     logging.info(f"Статус: {response.status_code}")
    #     if response.status_code == 200:
    #         logging.info("Прогнозирование выполнено успешно")

    
    

    raw_data_paths = get_up_to_date_data()
    prep_data_paths = prepare_data(raw_data_paths)
    upd = update_analytic_tables(prep_data_paths)
    # forecast = make_forecast()

    raw_data_paths >> prep_data_paths >> upd
    # raw_data_paths >> prep_data_paths >> upd >> forecast


analytical_dag_var = analytical_dag() 