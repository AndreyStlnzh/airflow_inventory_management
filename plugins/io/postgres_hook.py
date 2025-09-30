from airflow.hooks.base import BaseHook
from etl.clients.postgres_client import PostgresClient


class PostgresHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id

    def get_client(self) -> PostgresClient:
        connection = self.get_connection(self.conn_id)

        return PostgresClient(
            database=connection.schema,
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
        )