USE ecom;

-- 1. Топ-20 категорий по количеству товаров (используем MV)
SELECT category_id, countMerge(total_offers) as total 
FROM catalog_by_category_stats 
GROUP BY category_id 
ORDER BY total DESC 
LIMIT 20;

-- 2. Топ-30 брендов по количеству товаров (используем MV)
SELECT vendor, countMerge(total_offers) as total 
FROM catalog_by_brand_stats 
GROUP BY vendor 
ORDER BY total DESC 
LIMIT 30;

-- 3. Среднее количество товаров на один бренд в каждой категории
SELECT category_id, count(DISTINCT offer_id) / count(DISTINCT vendor) as avg_offers_per_brand
FROM ecom_offers
WHERE vendor IS NOT NULL
GROUP BY category_id
ORDER BY avg_offers_per_brand DESC
LIMIT 10;

-- 4. Нахождение товаров, по которым не было ни одного события (LEFT ANTI JOIN)
-- Считаем количество таких "сиротских" товаров
SELECT count() as orphan_offers_count
FROM ecom_offers AS o
LEFT ANTI JOIN raw_events AS e ON o.offer_id = e.ContentUnitID;
