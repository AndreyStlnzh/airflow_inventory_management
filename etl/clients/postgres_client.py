import logging
import pandas as pd
import psycopg2

from typing import List, Tuple


class PostgresClient:
    def __init__(
        self,
        database: str,
        user: str,
        password: str,
        host: str,
        port: int
    ):
        self.conn_params  = {
            'dbname': database,
            'user': user,
            'password': password,
            'port': port,
            'host': host
        }

    def _get_conn(self):
        """Создаёт новое подключение"""
        return psycopg2.connect(**self.conn_params)


    def _get_item_id_by_uuid(
        self,
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
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (external_uuid, ))
                res = cursor.fetchone()
                return res[0] if res else None


    def truncate_table(
        self,
        table_name: str,
    ) -> None:
        """
        Функция очистки таблиц
        На момент написания используется для очитски таблицы рецептов
        для пополнения актуальными данными
        """
        # TODO: Возможно очистка и пополнение будет трудоемким процессом и 
        # нужно будет сделать так же, как с блюдами, по external_uuid
        query = f"""
            TRUNCATE {table_name}
        """
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
        logging.info(f"The table {table_name} has been truncated successfully")


    def sync_table(
        self,
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

        # TODO: Перенести mapped в prepare, убрать accordance

        # Маппинг входных данных
        mapped = [
            {my_col: row[ext_col] for ext_col, my_col in accordance.items()}
            for row in data
        ]
        logging.info(mapped)
        if category_fk: # Если необходимо добавить внешний ключ по uuid категории
            [i.update({"category": self._get_item_id_by_uuid("category", i["category"])}) for i in mapped]

        # TODO Взять названия из первого словаря
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

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, mapped)
            conn.commit()

        logging.info(f"The table {table_name} synchronized successfully")


    def update_is_active_field(
        self,
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

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (up_to_date_dish_uuids,))
            conn.commit()
        logging.info(f"is_active field successfully updated")


    def insert_analytic_data(
        self,
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
                self._get_item_id_by_uuid(fk, row[f"{fk}_id"]), axis=1)
        
        logging.info("DataFrame updated with item IDs")

        query = f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders});
        """

        values: List[Tuple] = list(data_df.itertuples(index=False, name=None))

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, values)
            conn.commit()

        logging.info(f"The table {table_name} has been updated successfully")
