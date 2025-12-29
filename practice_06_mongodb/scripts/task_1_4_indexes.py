from pymongo import MongoClient, TEXT, ASCENDING, DESCENDING

client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']

print("Создание индексов для коллекции categories...")
# 1. Текстовый индекс на поле path
db.categories.create_index([("path", TEXT)])
# 2. Индекс на поле path_array (для подкатегорий)
db.categories.create_index("path_array")
# 3. Составной индекс на partner и level
db.categories.create_index([("partner", ASCENDING), ("level", ASCENDING)])
# 4. Индекс на metadata.total_products
db.categories.create_index([("metadata.total_products", DESCENDING)])

print("Создание индексов для коллекции products...")
# 1. Составной индекс на partner и category.id
db.categories.create_index([("partner", ASCENDING), ("category.id", ASCENDING)])
# 2. Индекс на хлебные крошки
db.products.create_index("category.breadcrumbs.name")
# 3. Составной индекс на type и partner
db.products.create_index([("type", ASCENDING), ("partner", ASCENDING)])
# 4. Индекс на offer_id
db.products.create_index("offer_id")

print("\nСписок индексов коллекции categories:")
for idx in db.categories.list_indexes():
    print(idx)

print("\nСписок индексов коллекции products:")
for idx in db.products.list_indexes():
    print(idx)

# Расчет размера индексов
stats_prod = db.command("collstats", "products")
print(f"\nРазмер данных (products): {stats_prod['size'] / 1024**2:.2f} MB")
print(f"Размер индексов (products): {stats_prod['totalIndexSize'] / 1024**2:.2f} MB")