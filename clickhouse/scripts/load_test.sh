#!/bin/bash

# Конфигурация
CH_CLIENT="docker exec -i clickhouse clickhouse-client --user admin --password admin -n"
CONCURRENCY=8  # Количество параллельных потоков
DURATION=300    # Длительность теста в секундах

echo "Запуск имитации нагрузки на ClickHouse ($CONCURRENCY потоков)..."

# Функция, которую будет выполнять каждый поток
run_load() {
    local end=$((SECONDS + DURATION))
    while [ $SECONDS -lt $end ]; do
        # Смесь легких и тяжелых запросов для имитации реальной работы
        $CH_CLIENT <<EOF
-- Тяжелый запрос: JOIN и агрегация по всем данным
SELECT vendor, count() as c 
FROM ecom.ecom_offers 
GROUP BY vendor 
ORDER BY c DESC 
LIMIT 10;

-- Запрос к событиям с фильтрацией
SELECT ProvinceName, count() 
FROM ecom.raw_events 
WHERE Hour > 10 
GROUP BY ProvinceName;

-- Специальный "медленный" запрос для появления в "Активных запросах"
SELECT sleep(1), count() FROM ecom.ecom_offers;
EOF
    done
}

# Запуск параллельных процессов
for ((i=1; i<=CONCURRENCY; i++)); do
    run_load &
done

echo "Нагрузка запущена. Наблюдайте за дашбордом Grafana."
echo "Скрипт завершится автоматически через $DURATION секунд."

# Ожидание завершения всех фоновых процессов
wait
echo "Тест завершен."