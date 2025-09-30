from airflow.hooks.base import BaseHook

from etl.clients.minio_client import MinioClient


class MinioHook(BaseHook):
    def __init__(self, conn_id: str, bucket_name: str):
        super().__init__()
        self.conn_id = conn_id
        self.bucket_name = bucket_name

    def get_client(self) -> MinioClient:
        connection = self.get_connection(self.conn_id)
        return MinioClient(
            minio_config = {
                "endpoint": connection.extra_dejson["endpoint_url"],
                "access_key": connection.extra_dejson["aws_access_key_id"],
                "secret_key": connection.extra_dejson["aws_secret_access_key"],
                "secure": False
            },
            bucket_name=self.bucket_name,
        )
