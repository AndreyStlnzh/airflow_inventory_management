import minio
import pandas as pd
import logging
from io import BytesIO
from typing_extensions import Literal
from datetime import datetime

from etl.serializers.dataframe import bytes_to_dataframe, dataframe_to_bytes

class MinioClient:
    def __init__(
        self, 
        minio_config: dict, 
        bucket_name: str
    ):
        self._minio_client = minio.Minio(
            **minio_config
        )

        if not self._minio_client.bucket_exists(bucket_name):
            self._minio_client.make_bucket(bucket_name)

        self._bucket_name = bucket_name

    def upload(
        self,
        data_df: pd.DataFrame,
        format: Literal["csv", "parquet"],
        folder: str = "",
    ) -> str:
        logging.info("Data serialization...")
        data_bytes = dataframe_to_bytes(dataframe=data_df, format="parquet")

        logging.info("Uplouding raw data minio...")
        minio_path = f"{folder}data_{datetime.now().\
                isoformat(timespec='seconds')}.{format}"

        self._minio_client.put_object(
            bucket_name=self._bucket_name,
            object_name=minio_path,
            data=BytesIO(data_bytes),
            length=len(data_bytes)
        )
        logging.info(f"The data is uploaded to minio: {minio_path}")
        return minio_path
    

    def download(
        self,
        filename: str,
        format: Literal["csv", "parquet"],
    ) -> pd.DataFrame:
        logging.info(f"\nReading from a minio, file: {filename}")
        response = self._minio_client.get_object(self._bucket_name, filename)
        data_bytes = response.read()
        logging.info("The data is downloaded from the minio. The beginning of deserialization")
        return bytes_to_dataframe(file_bytes=data_bytes, format=format)