from pymongo import MongoClient
import pandas as pd
import time

client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll = db['products']

def run_aggregation(title, pipeline):
    print(f"\n{'='*20}")
    print(f"ВЫПОЛНЕНИЕ: {title}")
    print(f"{'='*20}")
    
    start_time = time.time()
    results = list(coll.aggregate(pipeline))
    execution_time = time.time() - start_time
    
    if not results:
        print("Результаты не найдены.")
        return execution_time

    # Обработка результатов для вывода
    flat_results = []
    for res in results:
        # Если _id это словарь (как во 2-й агрегации), разворачиваем его
        if isinstance(res.get('_id'), dict):
            item = res.copy()
            for k, v in res['_id'].items():
                item[k] = v
            del item['_id']
            flat_results.append(item)
        else:
            flat_results.append(res)
    
    df = pd.DataFrame(flat_results)
    print(df.to_string(index=False))
    print(f"\nВремя выполнения: {execution_time:.4f} сек")
    return execution_time

# --- Агрегация 1: Топ-10 категорий ---
pipeline_1 = [
    {"$group": {
        "_id": "$category.id",
        "category_name": {"$first": "$category.name"},
        "full_path": {"$first": "$category.full_path"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 10},
    {"$project": {
        "category_id": "$_id",
        "category_name": 1,
        "full_path": 1,
        "count": 1,
        "_id": 0
    }}
]

# --- Агрегация 2: Иерархическая статистика ---
pipeline_2 = [
    {"$unwind": "$category.breadcrumbs"},
    {"$group": {
        "_id": {
            "level": "$category.breadcrumbs.level",
            "name": "$category.breadcrumbs.name"
        },
        "total_products": {"$sum": 1}
    }},
    {"$sort": {"_id.level": 1, "total_products": -1}},
    {"$limit": 30}
]

time_1 = run_aggregation("Агрегация 1: Топ-10 категорий", pipeline_1)
time_2 = run_aggregation("Агрегация 2: Иерархическая статистика", pipeline_2)