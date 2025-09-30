from airflow.hooks.S3_hook import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from etl.clients.api_client import APIClient
from plugins.io.minio_hook import MinioHook
from plugins.io.postgres_hook import PostgresHook
from plugins.io.api_hook import APIHook


api_hook = APIHook(conn_id="1C_api")
api_client: APIClient = api_hook.get_client()


data = api_client.get_categories_data()
print(type(data))
print(data)
