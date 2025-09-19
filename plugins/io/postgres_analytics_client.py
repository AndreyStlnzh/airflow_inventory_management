import logging
from typing import List, Tuple
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.io.postgres_add_func import get_item_id_by_uuid


def insert_data(
    data_df: pd.DataFrame, 
    table_name: str,
    fk_on: List[str]
) -> None:
    """
    Функция сохранения аналитических данных в БД.

    data_df: pd.DataFrame - данные для сохранения
    table_name: str - имя таблицы для сохранения данных
    fk_on: List[str] - список имен таблиц, по которым нужно установить внешние ключи
    например, ["dish", "product"]. Названия столбцов с первичными ключами должны быть
    в формате {table_name}_id, например, dish_id, product_id. К ним будут установлены
    внешние ключи по uuid из соответствующих таблиц.
    """
    if data_df.empty:
        logging.info("No data to save.")
        return
    
    columns = data_df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(columns))

    for fk in fk_on:
        if f"{fk}_id" not in columns:
            raise ValueError(f"Foreign key column '{fk}_id' not found in DataFrame columns")
        
        # Ограничение - названия столбцов с primary key должны быть в формате {table_name}_id
        data_df[f"{fk}_id"] = data_df.apply(lambda row: \
            get_item_id_by_uuid(fk, row[f"{fk}_id"]), axis=1)
    
    logging.info("DataFrame updated with item IDs")

    query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({placeholders});
    """

    values: List[Tuple] = list(data_df.itertuples(index=False, name=None))

    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, values)
        conn.commit()

    logging.info(f"The table {table_name} has been updated successfully")
