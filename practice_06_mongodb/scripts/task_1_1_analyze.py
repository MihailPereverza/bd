import pandas as pd
import os

# Путь к файлу (согласно твоей структуре)
file_path = 'data/catalog_data.parquet'

if not os.path.exists(file_path):
    print(f"Ошибка: Файл {file_path} не найден!")
else:
    df = pd.read_parquet(file_path)
    
    print("=== АНАЛИЗ ДАННЫХ ДЛЯ ЗАДАНИЯ 1.1 ===")
    
    # 1. Уникальные категории
    unique_cats = df['Category_FullPathName'].unique()
    print(f"1. Уникальных категорий: {len(unique_cats)}")
    
    # 2. Максимальная глубина вложенности
    # В условии сказано разделитель \, проверим на всякий случай
    depths = df['Category_FullPathName'].apply(lambda x: len(str(x).split('\\')))
    print(f"2. Максимальная глубина вложенности: {depths.max()}")
    
    # 3. Категории с наибольшим количеством товаров
    top_categories = df['Category_FullPathName'].value_counts().head(5)
    print(f"\n3. Топ-5 категорий по количеству товаров:")
    print(top_categories)
    
    # 4. Проверка дубликатов Offer_ID у разных партнеров
    # Группируем по Offer_ID и считаем количество уникальных Partner_Name
    offer_partners = df.groupby('Offer_ID')['Partner_Name'].nunique()
    shared_offers = offer_partners[offer_partners > 1]
    print(f"\n4. Количество Offer_ID, встречающихся у нескольких партнеров: {len(shared_offers)}")
    
    # 5. Уникальные типы товаров
    unique_types = df['Offer_Type'].nunique()
    print(f"5. Уникальных типов товаров (Offer_Type): {unique_types}")
    
    print("\n=== КОНЕЦ ОТЧЕТА ===")