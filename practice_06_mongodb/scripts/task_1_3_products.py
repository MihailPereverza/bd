import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll_products = db['products']

# Очистка перед загрузкой
coll_products.delete_many({})

# Чтение данных
file_path = 'data/catalog_data.parquet'
df = pd.read_parquet(file_path)

products_to_insert = []
now = datetime.now()

print("Начинаю трансформацию и загрузку товаров...")

for _, row in df.iterrows():
    partner = row['Partner_Name']
    offer_id = str(row['Offer_ID'])
    
    # Обработка пути категории
    full_path_raw = str(row['Category_FullPathName'])
    normalized_path = full_path_raw.replace('\\', '/')
    path_elements = normalized_path.split('/')
    
    # Формируем массив breadcrumbs
    breadcrumbs = []
    for i, level_name in enumerate(path_elements):
        breadcrumbs.append({
            "level": i + 1,
            "name": level_name
        })
    
    # Формируем документ по ТЗ
    doc = {
        "_id": f"{partner}_{offer_id}",
        "partner": partner,
        "offer_id": offer_id,
        "name": row['Offer_Name'],
        "type": row['Offer_Type'],
        "category": {
            "id": str(row['Category_ID']),
            "name": path_elements[-1],
            "full_path": normalized_path,
            "breadcrumbs": breadcrumbs
        },
        "created_at": now,
        "updated_at": now
    }
    products_to_insert.append(doc)
    
    # Вставка порциями по 5000 документов для стабильности
    if len(products_to_insert) >= 5000:
        coll_products.insert_many(products_to_insert)
        products_to_insert = []

# Вставка остатка
if products_to_insert:
    coll_products.insert_many(products_to_insert)

print("Загрузка завершена!")

# --- Сбор статистики для отчета ---
total_count = coll_products.count_documents({})

print(f"\n=== СТАТИСТИКА ДЛЯ ЗАДАНИЯ 1.3 ===")
print(f"1. Общее количество товаров: {total_count}")

print("\n2. Топ-5 типов товаров по частоте:")
top_types = coll_products.aggregate([
    {"$group": {"_id": "$type", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
])
for t in top_types:
    print(f" - {t['_id']}: {t['count']}")

print("\n3. Распределение товаров по партнерам:")
partner_dist = coll_products.aggregate([
    {"$group": {"_id": "$partner", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
])
for p in partner_dist:
    print(f" - {p['_id']}: {p['count']}")