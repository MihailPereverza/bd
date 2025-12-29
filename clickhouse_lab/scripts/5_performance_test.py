import time
import clickhouse_connect

# Подключаемся с созданным пользователем
try:
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123, 
        username='admin',
        password='admin'
    )
except Exception as e:
    print(f"Ошибка подключения: {e}")
    exit(1)

def run_benchmark(query, name, iterations):
    print(f"Запуск теста для: {name} ({iterations} итераций)")
    start_time = time.time()
    
    for i in range(iterations):
        client.query(query)
    
    end_time = time.time()
    total_time = end_time - start_time
    avg_latency = (total_time / iterations) * 1000
    qps = iterations / total_time
    
    print(f"  Результаты:")
    print(f"  - Общее время: {total_time:.4f} сек")
    print(f"  - Средняя задержка: {avg_latency:.2f} мс")
    print(f"  - Запросов в секунду (QPS): {qps:.2f}\n")
    return avg_latency, qps

query_raw = "SELECT category_id, count(offer_id) FROM ecom.ecom_offers GROUP BY category_id ORDER BY count() DESC LIMIT 10"
query_mv = "SELECT category_id, countMerge(total_offers) FROM ecom.catalog_by_category_stats GROUP BY category_id ORDER BY countMerge(total_offers) DESC LIMIT 10"

print("--- СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ---\n")
mv_latency, mv_qps = run_benchmark(query_mv, "Materialized View", 100)
raw_latency, raw_qps = run_benchmark(query_raw, "Raw Data (Full Scan)", 5)

acceleration = raw_latency / mv_latency
print(f"ИТОГ: Materialized View работает в {acceleration:.1f} раз быстрее!")
