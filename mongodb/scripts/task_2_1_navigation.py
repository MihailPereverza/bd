from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['ecom_catalog']
coll = db['categories']

def analyze_query(title, filter_query, sort_query=None):
    print(f"\n{'='*20}")
    print(f"ВЫПОЛНЕНИЕ: {title}")
    print(f"{'='*20}")
    
    # 1. Считаем количество
    count = coll.count_documents(filter_query)
    print(f"Количество найденных документов: {count}")
    
    # 2. Получаем первые 3 примера
    cursor = coll.find(filter_query)
    if sort_query:
        cursor = cursor.sort(sort_query)
    
    print("Первые 3 результата:")
    for doc in cursor.limit(3):
        print(f" - Название: {doc['name']} | Товаров: {doc['metadata']['total_products']}")
    
    # 3. Анализ индекса (explain)
    # Используем cursor.explain() для анализа плана выполнения
    explanation = coll.find(filter_query).sort(sort_query if sort_query else [('_id', 1)]).explain()
    winning_plan = explanation['queryPlanner']['winningPlan']
    
    # Рекурсивно ищем имя индекса в плане
    def find_index_name(plan):
        if 'indexName' in plan:
            return plan['indexName']
        if 'inputStage' in plan:
            return find_index_name(plan['inputStage'])
        if 'inputStages' in plan:
            for stage in plan['inputStages']:
                name = find_index_name(stage)
                if name: return name
        return "COLLSCAN (ИНДЕКС НЕ ИСПОЛЬЗУЕТСЯ)"

    idx_used = find_index_name(winning_plan)
    print(f"Использование индекса: {idx_used}")

# --- Запуск запросов ---

# Запрос 1: Корневые категории
analyze_query(
    "Запрос 1: Корневые категории (level 1) для _ozon",
    {"level": 1, "partner": "_ozon"}
)

# Запрос 2: Все подкатегории 'Строительство и ремонт'
analyze_query(
    "Запрос 2: Подкатегории 'Строительство и ремонт'",
    {"path_array": "Строительство и ремонт"}
)

# Запрос 3: Топ-10 по количеству товаров
analyze_query(
    "Запрос 3: Топ-10 самых населенных категорий",
    {}, 
    [("metadata.total_products", -1)]
)