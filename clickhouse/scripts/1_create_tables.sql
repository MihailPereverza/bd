CREATE DATABASE IF NOT EXISTS ecom;

USE ecom;

-- 1. Таблица каталога товаров
-- Используем ReplacingMergeTree для схлопывания дублей по offer_id
CREATE TABLE IF NOT EXISTS ecom_offers (
    offer_id UInt64,
    price Float32,
    seller_id UInt64,
    category_id UInt64,
    vendor Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY offer_id;

-- 2. Таблица событий
-- Партиционируем по часам, сортируем по полям фильтрации
CREATE TABLE IF NOT EXISTS raw_events (
    hour UInt8,
    DeviceTypeName String,
    ApplicationName String,
    OSName String,
    ProvinceName String,
    ContentUnitID Nullable(UInt64)
) ENGINE = MergeTree()
PARTITION BY hour
ORDER BY (hour, ApplicationName, DeviceTypeName);
