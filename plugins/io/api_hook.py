from airflow.hooks.base import BaseHook
from etl.clients.api_client import APIClient


class APIHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id

    def get_client(self) -> APIClient:
        connection = self.get_connection(self.conn_id)
        base_url = f"http://{connection.host}:{connection.port}"
        return APIClient(base_url)
