import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll_categories = db['categories']

# Очистка коллекции перед запуском
coll_categories.delete_many({})

# Загрузка данных
df = pd.read_parquet('data/catalog_data.parquet')

# Выделяем уникальные категории для каждого партнера
unique_cats = df.groupby(['Partner_Name', 'Category_ID', 'Category_FullPathName']).size().reset_index(name='total_products')

categories_to_insert = []

for _, row in unique_cats.iterrows():
    partner = row['Partner_Name']
    cat_id = row['Category_ID']
    full_path_str = row['Category_FullPathName']
    
    # Преобразуем разделитель \ в / как в ТЗ
    normalized_path = full_path_str.replace('\\', '/')
    path_elements = normalized_path.split('/')
    
    # Формируем документ
    doc = {
        "_id": f"{partner}_{cat_id}",
        "partner": partner,
        "category_id": str(cat_id),
        "name": path_elements[-1], # Последний элемент — название
        "path": normalized_path,
        "path_array": path_elements,
        "level": len(path_elements),
        "parent_path": "/".join(path_elements[:-1]) if len(path_elements) > 1 else None,
        "metadata": {
            "total_products": int(row['total_products']),
            "last_updated": datetime.now()
        }
    }
    categories_to_insert.append(doc)

# Массовая вставка
if categories_to_insert:
    coll_categories.insert_many(categories_to_insert)

print(f"Загружено категорий: {len(categories_to_insert)}")

# Статистика по уровням
stats = coll_categories.aggregate([
    {"$group": {"_id": "$level", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
])

print("\nРаспределение по уровням:")
for s in stats:
    print(f"Уровень {s['_id']}: {s['count']} категорий")