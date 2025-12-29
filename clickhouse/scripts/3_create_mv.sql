USE ecom;

-- 1. Агрегаты по категориям
CREATE TABLE IF NOT EXISTS catalog_by_category_stats (
    category_id UInt64,
    total_offers AggregateFunction(count, UInt64),
    avg_price AggregateFunction(avg, Float32)
) ENGINE = AggregatingMergeTree() 
ORDER BY category_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS catalog_by_category_mv
TO catalog_by_category_stats
AS SELECT 
    category_id, 
    countState(offer_id) AS total_offers,
    avgState(price) AS avg_price
FROM ecom_offers
GROUP BY category_id;

-- 2. Агрегаты по брендам (vendor)
CREATE TABLE IF NOT EXISTS catalog_by_brand_stats (
    vendor String,
    total_offers AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY vendor;

CREATE MATERIALIZED VIEW IF NOT EXISTS catalog_by_brand_mv
TO catalog_by_brand_stats
AS SELECT 
    ifNull(vendor, 'Unknown') as vendor, 
    countState(offer_id) AS total_offers
FROM ecom_offers
GROUP BY vendor;

-- 3. Наполнение MV историческими данными 
-- (Так как данные уже в таблицах, нам нужно "прогреть" MV)
INSERT INTO catalog_by_category_stats
SELECT category_id, countState(offer_id), avgState(price)
FROM ecom_offers GROUP BY category_id;

INSERT INTO catalog_by_brand_stats
SELECT ifNull(vendor, 'Unknown'), countState(offer_id)
FROM ecom_offers GROUP BY vendor;
