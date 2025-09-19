# table_name = "category"
# conflict_key = "external_uuid"

# data = [
#         {"guid": "678fe344-297b-49af-a1cc-27750f4b972f", "category_name": "Первые блюда", "type": "dish"},
#         {"guid": "5763451e-24b9-49ef-b6bb-d3ceb87cadc3", "category_name": "Вторые блюда", "type": "dish"},
#         {"guid": "769f168d-dac3-4c51-8315-0236b8e25881", "category_name": "Овощи", "type": "product"},
#         {"guid": "f8b35ac9-32cc-4f7d-8c8c-d9fe57d222aa", "category_name": "Крупы", "type": "product"},
#         {"guid": "f3d74f11-d900-4e47-85c8-31d05e2ad423", "category_name": "Мясо", "type": "product"},
#         {"guid": "dca63de3-9681-44c2-b636-255138802218", "category_name": "Специи", "type": "product"},
#     ]


# accordance = {
#     "name": "category_name",
#     "type": "type",
#     "external_uuid": "guid",
# }

# # Маппинг входных данных
# mapped = [
#     {my_col: row[ext_col] for my_col, ext_col in accordance.items()}
#     for row in data
# ]

# print(mapped)

# columns = list(accordance.keys())  # наши колонки в БД
# placeholders = ", ".join([f"%({col})s" for col in columns])

# update_clause = ", ".join(
#     [f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_key]
# )

# query = f"""
#     INSERT INTO {table_name} ({", ".join(columns)})
#     VALUES ({placeholders})
#     ON CONFLICT ({conflict_key}) DO UPDATE
#     SET {update_clause};
# """

# print(query)


sales_data = [
        {"date": "2025-08-26", "dish_id": 1, "quantity": 90},
        {"date": "2025-08-26", "dish_id": 3, "quantity": 15},
        {"date": "2025-08-26", "dish_id": 4, "quantity": 55},
        {"date": "2025-08-26", "dish_id": 2, "quantity": 40},
        {"date": "2025-08-27", "dish_id": 1, "quantity": 95},
        {"date": "2025-08-27", "dish_id": 2, "quantity": 45},
        {"date": "2025-08-27", "dish_id": 3, "quantity": 18},
        {"date": "2025-08-27", "dish_id": 4, "quantity": 58},
        {"date": "2025-08-28", "dish_id": 1, "quantity": 98},
        {"date":  "2025-08-28", "dish_id": 2, "quantity": 48},
        {"date":  "2025-08-28", "dish_id": 3, "quantity": 19},
        {"date":  "2025-08-28", "dish_id": 4, "quantity": 59},
        {"date":  "2025-08-29", "dish_id": 1, "quantity": 97},
        {"date":  "2025-08-29", "dish_id": 2, "quantity": 47},
        {"date":  "2025-08-29", "dish_id": 3, "quantity": 17},
        {"date":  "2025-08-29", "dish_id": 4, "quantity": 57},
        {"date":  "2025-08-30", "dish_id": 1, "quantity": 100},
        {"date":  "2025-08-30", "dish_id": 2, "quantity": 50},
        {"date":  "2025-08-30", "dish_id": 3, "quantity": 20},
        {"date":  "2025-08-30", "dish_id": 4, "quantity": 60},
        {"date":  "2025-08-31", "dish_id": 1, "quantity": 102},
        {"date":  "2025-08-31", "dish_id": 2, "quantity": 52},
        {"date":  "2025-08-31", "dish_id": 3, "quantity": 21},
        {"date":  "2025-08-31", "dish_id": 4, "quantity": 61},
        {"date":  "2025-09-01", "dish_id": 1, "quantity": 101},
        {"date":  "2025-09-01", "dish_id": 2, "quantity": 51},
        {"date":  "2025-09-01", "dish_id": 3, "quantity": 22},
        {"date":  "2025-09-01", "dish_id": 4, "quantity": 62},
        {"date":  "2025-09-02", "dish_id": 1, "quantity": 49},
        {"date":  "2025-09-02", "dish_id": 2, "quantity": 49},
        {"date":  "2025-09-02", "dish_id": 3, "quantity": 49},
        {"date":  "2025-09-02", "dish_id": 4, "quantity": 49},
    ]
