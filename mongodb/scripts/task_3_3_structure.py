from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll = db['categories']

print("=== ВЫПОЛНЕНИЕ ЗАДАНИЯ 3.3 ===")

# --- Агрегация А: Распределение по партнерам и уровням ---
pipeline_a = [
    {
        "$group": {
            "_id": {
                "partner": "$partner",
                "level": "$level"
            },
            "count_categories": {"$sum": 1},
            "total_items": {"$sum": "$metadata.total_products"}
        }
    },
    {
        "$sort": {
            "_id.partner": 1,
            "_id.level": 1
        }
    },
    {
        "$project": {
            "partner": "$_id.partner",
            "level": "$_id.level",
            "count_categories": 1,
            "total_items": 1,
            "_id": 0
        }
    }
]

# --- Агрегация Б: Категории-листья через $lookup ---
pipeline_b = [
    {
        "$lookup": {
            "from": "categories",
            "localField": "path",
            "foreignField": "parent_path",
            "as": "children"
        }
    },
    # Оставляем только те, у кого массив детей пуст
    {
        "$match": {
            "children": {"$size": 0}
        }
    },
    {
        "$sort": {"metadata.total_products": -1}
    },
    {
        "$limit": 10
    },
    {
        "$project": {
            "name": 1,
            "level": 1,
            "total_products": "$metadata.total_products",
            "path": 1,
            "_id": 0
        }
    }
]

print("\nРЕЗУЛЬТАТ АГРЕГАЦИИ А (Распределение по уровням):")
res_a = list(coll.aggregate(pipeline_a))
df_a = pd.DataFrame(res_a)
print(df_a.to_string(index=False))

print("\nРЕЗУЛЬТАТ АГРЕГАЦИИ Б (Топ-10 листьев):")
res_b = list(coll.aggregate(pipeline_b))
df_b = pd.DataFrame(res_b)
print(df_b.to_string(index=False))

# Вычисление средней глубины для вывода
avg_depth = coll.aggregate([{"$group": {"_id": None, "avg": {"$avg": "$level"}}}])
print(f"\nСредняя глубина категорий: {list(avg_depth)[0]['avg']:.2f}")