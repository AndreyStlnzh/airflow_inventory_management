from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

def get_item_id_by_uuid(
    item: str,
    external_uuid: str,
) -> int:
    """
    Функция получения id элемента по его внешнему uuid.
    """
    query = f"""
        SELECT {item}_id
        FROM {item}
        WHERE external_uuid = %s;
    """
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (external_uuid, ))
            res = cursor.fetchone()
            return res[0] if res else None