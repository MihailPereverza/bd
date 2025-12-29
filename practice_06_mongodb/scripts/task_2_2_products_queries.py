from pymongo import MongoClient
import pandas as pd
import json

# Подключение к БД
client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll = db['products']

def print_section(title):
    print(f"\n{'='*50}")
    print(f"{title}")
    print(f"{'='*50}")

# --- Запрос 1: Степлеры в Пневмоинструментах ---
print_section("Запрос 1: Степлеры в Пневмоинструментах")
query_1 = {
    "type": "Степлер строительный",
    "category.breadcrumbs.name": "Пневмоинструменты"
}
count_1 = coll.count_documents(query_1)
# Получаем 2 примера
examples_1 = list(coll.find(query_1, {"_id": 0, "offer_id": 1, "name": 1, "type": 1}).limit(2))

print(f"Количество найденных документов: {count_1}")
print("Примеры документов (2 шт):")
for doc in examples_1:
    print(json.dumps(doc, ensure_ascii=False, indent=2))


# --- Запрос 2: 4-й уровень иерархии ---
print_section("Запрос 2: Товары на 4-м уровне иерархии")
# Проверка существования 4-го элемента (индекс 3) в массиве breadcrumbs
query_2 = {"category.breadcrumbs.3": {"$exists": True}}
count_2 = coll.count_documents(query_2)
# Получаем 2 примера с полным путем для наглядности
examples_2 = list(coll.find(query_2, {
    "_id": 0,
    "name": 1,
    "category.full_path": 1
}).limit(2))

print(f"Количество найденных документов: {count_2}")
print("Примеры документов (2 шт):")
for doc in examples_2:
    print(json.dumps(doc, ensure_ascii=False, indent=2))


# --- Запрос 3: Агрегация по 1-му уровню ---
print_section("Запрос 3: Распределение по категориям 1-го уровня")
pipeline = [
    {
        "$project": {
            "root_category": {"$arrayElemAt": ["$category.breadcrumbs.name", 0]}
        }
    },
    {
        "$group": {
            "_id": "$root_category",
            "total_count": {"$sum": 1}
        }
    },
    {"$sort": {"total_count": -1}}
]
results_3 = list(coll.aggregate(pipeline))

# Преобразуем в таблицу Pandas для скриншота
df = pd.DataFrame(results_3).rename(columns={"_id": "Корневая категория (Level 1)", "total_count": "Кол-во товаров"})
print(df.to_string(index=False))