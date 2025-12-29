#!/bin/bash
# ЭКСТРЕМАЛЬНАЯ НАГРУЗКА на PostgreSQL
# ВНИМАНИЕ: Это создаст МАКСИМАЛЬНУЮ нагрузку!

THREADS=${1:-20}
DURATION=${2:-300}  # секунды

echo "=========================================="
echo "ЭКСТРЕМАЛЬНАЯ НАГРУЗКА НА POSTGRESQL"
echo "=========================================="
echo "Потоков: $THREADS"
echo "Длительность: ${DURATION} секунд"
echo "=========================================="
echo ""

# Функция экстремальной нагрузки
extreme_load() {
    local thread_id=$1
    
    local end_time=$(($(date +%s) + $DURATION))
    local iteration=0
    
    while [ $(date +%s) -lt $end_time ]; do
        iteration=$((iteration + 1))
        
        # Массовые вставки
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        INSERT INTO myapp.users (email, username)
        SELECT 
            'extreme_' || $thread_id || '_' || extract(epoch from now())::bigint || '_' || generate_series || '@extreme.com',
            'extremeuser_' || $thread_id || '_' || extract(epoch from now())::bigint || '_' || generate_series
        FROM generate_series(1, 100)
        ON CONFLICT (email) DO NOTHING;
        " > /dev/null 2>&1 &
        
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        INSERT INTO myapp.orders (user_id, order_number, status, total_amount)
        SELECT 
            (SELECT id FROM myapp.users ORDER BY random() LIMIT 1),
            'EXTREME-' || $thread_id || '-' || extract(epoch from now())::bigint || '-' || generate_series,
            (ARRAY['pending', 'completed', 'processing'])[floor(random() * 3 + 1)],
            random() * 3000 + 10
        FROM generate_series(1, 50)
        ON CONFLICT (order_number) DO NOTHING;
        " > /dev/null 2>&1 &
        
        # Тяжелые запросы
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        SELECT 
            u.username, 
            COUNT(o.id), 
            SUM(o.total_amount),
            AVG(o.total_amount)
        FROM myapp.users u
        LEFT JOIN myapp.orders o ON u.id = o.user_id
        GROUP BY u.username
        ORDER BY SUM(o.total_amount) DESC NULLS LAST
        LIMIT 100;
        " > /dev/null 2>&1 &
        
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        SELECT 
            status,
            COUNT(*),
            SUM(total_amount),
            AVG(total_amount)
        FROM myapp.orders
        GROUP BY status;
        " > /dev/null 2>&1 &
        
        # Обновления
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        UPDATE myapp.orders 
        SET status = CASE 
            WHEN status = 'pending' THEN 'processing'
            WHEN status = 'processing' THEN 'completed'
            ELSE status
        END
        WHERE id IN (
            SELECT id FROM myapp.orders 
            WHERE status IN ('pending', 'processing')
            ORDER BY random()
            LIMIT 20
        );
        " > /dev/null 2>&1 &
        
        # Сложные аналитические запросы
        docker exec postgres-dev psql -U dbuser -d myapp -c "
        SELECT 
            oi.product_name,
            COUNT(*),
            SUM(oi.quantity * oi.price)
        FROM myapp.order_items oi
        JOIN myapp.orders o ON oi.order_id = o.id
        GROUP BY oi.product_name
        ORDER BY SUM(oi.quantity * oi.price) DESC
        LIMIT 50;
        " > /dev/null 2>&1 &
        
        if [ $((iteration % 50)) -eq 0 ]; then
            echo "[Поток $thread_id] Итерация $iteration"
        fi
        
        sleep 0.01
    done
    
    echo "[Поток $thread_id] Завершен после $iteration итераций"
}

# Запуск потоков
echo "Запуск $THREADS потоков экстремальной нагрузки..."
echo "Нажмите Ctrl+C для остановки"
echo ""

for thread in $(seq 1 $THREADS); do
    extreme_load $thread &
done

# Ожидание завершения
wait

echo ""
echo "Экстремальная нагрузка завершена!"

