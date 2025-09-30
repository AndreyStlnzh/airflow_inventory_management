import logging
import pandas as pd


sales_aggregation_group = ["date", "dish_id"]

def sales_raw_to_prepared(
    data_df: pd.DataFrame,
    accordance_dict: dict,
) -> pd.DataFrame:
    """
    Функция преобразования данных продаж
    Сырые данные -> подготовленные данные (как в нашей системе)
    Агрегирует данные по дням. Суммируются данные одного дня и блюда
    """
    data_df = data_df.rename(columns=accordance_dict)
    logging.info("The columns were renamed according to accordance")
    data_df = data_df.groupby(sales_aggregation_group, as_index=False).sum()
    logging.info("The data is aggregated by days and dishes")

    # .... Другие операции преобразования данных
    # Так как данные искусственые, преобразований не так много
    return data_df


def stock_raw_to_prepared(
    data_df: pd.DataFrame,
    accordance_dict: dict,
) -> pd.DataFrame:
    data_df = data_df.rename(columns=accordance_dict)
    logging.info("The columns were renamed according to accordance")
    # .... Другие операции преобразования данных
    return data_df
