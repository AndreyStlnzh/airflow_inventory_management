import logging

from psycopg2.extras import execute_values
from typing import List
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.io.postgres_add_func import get_item_id_by_uuid


def sync_table(
    table_name: str,
    data: List[dict],
    accordance: dict,
    conflict_key: str,
    category_fk: bool=False,
) -> None:
    """
    Функция синхроназации таблиц. Выполняет UPSERT

    accordance: dict - маппинг колонок внешних данных на наши колонки в
    conflict_key: str - колонка по которой будет проверяться конфликт
    таблице {наша колонка: внешняя колонка}
    category_fk: bool - если True, то будет добавлен внешний ключ по uuid категории
    """
    if not data:
        logging.info(f"No data to sync for {table_name}")
        return

    # Маппинг входных данных
    mapped = [
        {my_col: row[ext_col] for ext_col, my_col in accordance.items()}
        for row in data
    ]
    logging.info(mapped)
    if category_fk: # Если необходимо добавить внешний ключ по uuid категории
        [i.update({"category": get_item_id_by_uuid("category", i["category"])}) for i in mapped]


    columns = list(accordance.values())  # наши колонки в БД
    placeholders = ", ".join([f"%({col})s" for col in columns])

    update_clause = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_key]
    )

    query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_key}) DO UPDATE
        SET {update_clause};
    """

    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, mapped)
        conn.commit()

    logging.info(f"The table {table_name} synchronized successfully")


def update_is_active_field(
    data: List[dict],
    accordance: dict,
) -> None:
    """
    Функция обновление поля is_active в таблице dish на False
    для тех блюд, которых нет в новых данных
    """
    logging.info("Updating is_active field in dish table")
    query = f"""
        UPDATE DISH
        SET is_active = FALSE
        WHERE external_uuid NOT IN %s;
    """
    logging.info(data)
    # В row[""] указывается название столбца, указывающего на guid блюда
    # Поэтому в данной строке берется ключ по значению external_uuid (название в нашей системе)
    col_name = list(accordance.keys())[list(accordance.values()).index("external_uuid")]
    up_to_date_dish_uuids = tuple(row[col_name] for row in data)

    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (up_to_date_dish_uuids,))
        conn.commit()
    logging.info(f"is_active field successfully updated")


def truncate_table(
    table_name: str,
) -> None:
    query = f"""
        TRUNCATE {table_name}
    """
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
    logging.info(f"The table {table_name} has been truncated successfully")