import logging
import pandas as pd

from typing import Literal
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def uploud_to_minio(
    data_bytes: bytes,
    minio_bucket: str,
    format: Literal["csv", "parquet"],
    folder: str = "",
) -> str:
    """
    Функция загрузки DataFrame в minio в формате csv или parquet

    Args:
        data_bytes (bytes): Файл в формате байтов
        minio_bucket (str): название бакета
        format (Literal["csv", "parquet"]): Формат файла ('csv' или 'parquet')
        raw (bool): если True, то файл сохраняется в папку raw

    Returns:
        str: путь до загруженного файла в бакете
    """
    s3 = S3Hook('minio_conn')
    
    if not s3.check_for_bucket(minio_bucket):
        logging.info(f"Бакета {minio_bucket} не сущесвует, создаем")
        s3.create_bucket(minio_bucket)

    minio_path = f"{folder}data_{datetime.now().isoformat(timespec='seconds')}"

    if format == "csv":
        minio_path += ".csv"
    else: # parquet
        minio_path += ".parquet"

    s3.load_bytes(
        bytes_data=data_bytes, 
        key=minio_path,
        bucket_name=minio_bucket,
    )
    return minio_path


def download_from_minio(
    minio_path: str,
    minio_bucket: str,
) -> pd.DataFrame:
    """
    Функция скачивания csv файла с minio

    Args:
        minio_path (str): путь в бакете
        minio_bucket (str): название бакет
        format (Literal["csv", "parquet"]): Формат файла ('csv' или 'parquet')

    Returns:
        pd.DataFrame: скачанный DataFrame
    """
    s3 = S3Hook('minio_conn')

    obj = s3.get_key(
        key=minio_path, 
        bucket_name=minio_bucket,
    )
    file_bytes = obj.get()["Body"].read()
    
    return file_bytes
