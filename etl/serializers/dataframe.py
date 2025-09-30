from datetime import datetime
from io import BytesIO
import pandas as pd

from typing import Literal


def dataframe_to_bytes(
    dataframe: pd.DataFrame,
    format: Literal["csv", "parquet"]
) -> bytes:
    """
    Функция сериализации DataFrame в bytes
    """
    minio_path = f"data_{datetime.now().isoformat(timespec='seconds')}"
    if format == "csv":
        data_bytes = dataframe.to_csv().encode("utf-8")
        data_buffer = BytesIO(data_bytes)
        minio_path += ".csv"
    elif format == "parquet":
        data_bytes = dataframe.to_parquet(engine="pyarrow")
        data_buffer = BytesIO(data_bytes)
        minio_path += ".parquet"

    return data_buffer.getvalue()

def bytes_to_dataframe(
    file_bytes: bytes,
    format: Literal["csv", "parquet"]
) -> pd.DataFrame:
    if format == "csv":
        buffer = BytesIO(file_bytes)
        data_df = pd.read_csv(buffer)
        
    elif format == "parquet":
        buffer = BytesIO(file_bytes)
        data_df = pd.read_parquet(buffer)

    return data_df
