/**
 * ФИНАЛЬНЫЙ СКРИПТ АВТОМАТИЗАЦИИ
 * Настроены понятные заголовки для Stat-панелей и локализация осей.
 */

const CLICKHOUSE_UID = "af7scv6rf1ipsb";

const createDash = (name, title, spec) => {
  const payload = {
    "metadata": {
      "name": name,
      "namespace": "default"
    },
    "spec": {
      ...spec,
      "title": title
    }
  };

  fetch("/apis/dashboard.grafana.app/v1beta1/namespaces/default/dashboards", {
    "method": "POST",
    "headers": {
      "accept": "application/json",
      "content-type": "application/json",
      "x-grafana-org-id": "1"
    },
    "body": JSON.stringify(payload)
  })
  .then(res => res.json())
  .then(data => console.log(`Дашборд "${title}" создан успешно!`))
  .catch(err => console.error("Ошибка:", err));
};

// --- КОНФИГ 1: ТЕХНИЧЕСКИЙ МОНИТОРИНГ ---
const techSpec = {
  "editable": true,
  "panels": [
    { "id": 1, "title": "Интенсивность запросов", "type": "timeseries", "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 }, "datasource": { "type": "prometheus" }, "fieldConfig": { "defaults": { "custom": { "axisLabel": "Запросов в сек (QPS)" }, "unit": "reqps" } }, "targets": [{ "expr": "rate(ClickHouseMetrics_Query[1m])" }] },
    { "id": 2, "title": "Активные запросы", "type": "timeseries", "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 }, "datasource": { "type": "prometheus" }, "targets": [{ "expr": "ClickHouseMetrics_Query" }] },
    { "id": 3, "title": "Оперативная память", "type": "timeseries", "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 }, "datasource": { "type": "prometheus" }, "fieldConfig": { "defaults": { "unit": "bytes" } }, "targets": [{ "expr": "ClickHouseAsyncMetrics_MemoryResident" }] }
  ],
  "schemaVersion": 42
};

// --- КОНФИГ 2: БИЗНЕС-АНАЛИТИКА ---
const businessSpec = {
  "editable": true,
  "panels": [
    { 
      "id": 1, "title": "Топ-10 категорий товаров", "type": "barchart", 
      "gridPos": { "h": 10, "w": 12, "x": 0, "y": 0 }, 
      "datasource": { "uid": CLICKHOUSE_UID }, 
      "options": { "xTickLabelRotation": 15, "showValue": "auto", "groupWidth": 0.7 },
      "fieldConfig": { "defaults": { "custom": { "axisLabel": "Товаров в каталоге", "axisPlacement": "left" } } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT multiIf(category_id = 7508, 'Одежда', category_id = 15892, 'Электроника', category_id = 7660, 'Дом и сад', category_id = 7559, 'Детские товары', category_id = 36723, 'Красота', category_id = 7530, 'Бытовая техника', category_id = 17002, 'Книги', category_id = 7545, 'Спорт', concat('Кат. ', toString(category_id))) as `Категория`, countMerge(total_offers) as `товаров` FROM ecom.catalog_by_category_stats GROUP BY `Категория` ORDER BY `товаров` DESC LIMIT 10" }]
    },
    { 
      "id": 2, "title": "Топ-10 брендов", "type": "barchart", 
      "gridPos": { "h": 10, "w": 12, "x": 12, "y": 0 }, 
      "datasource": { "uid": CLICKHOUSE_UID }, 
      "options": { "xTickLabelRotation": 0, "showValue": "auto", "groupWidth": 0.7 },
      "fieldConfig": { "defaults": { "custom": { "axisLabel": "Товаров бренда", "axisPlacement": "left" } } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT vendor as `бренд`, countMerge(total_offers) as `товаров` FROM ecom.catalog_by_brand_stats WHERE `бренд` != 'Нет бренда' GROUP BY `бренд` ORDER BY `товаров` DESC LIMIT 10" }]
    },
    { 
      "id": 4, "title": "Доля товаров без бренда (Unknown)", "type": "stat", 
      "gridPos": { "h": 10, "w": 12, "x": 0, "y": 10 }, 
      "datasource": { "uid": CLICKHOUSE_UID }, 
      "fieldConfig": { "defaults": { "unit": "percent", "decimals": 2, "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": 0 }, { "color": "orange", "value": 20 }, { "color": "red", "value": 50 }] } } },
      "options": { "textMode": "value_and_name", "justifyMode": "center", "graphMode": "area" },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT (countIf(vendor = '' OR vendor = 'null' OR vendor IS NULL) / count(*)) * 100 as `Доля неизвестных брендов` FROM ecom.ecom_offers" }]
    },
    { 
      "id": 3, "title": "Охват каталога событиями", "type": "stat", 
      "gridPos": { "h": 10, "w": 12, "x": 12, "y": 10 }, 
      "datasource": { "uid": CLICKHOUSE_UID }, 
      "fieldConfig": { "defaults": { "unit": "percent", "decimals": 2, "thresholds": { "mode": "absolute", "steps": [{ "color": "red", "value": 0 }, { "color": "orange", "value": 1 }, { "color": "green", "value": 5 }] } } },
      "options": { "textMode": "value_and_name", "justifyMode": "center", "graphMode": "area" },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT (count(DISTINCT e.ContentUnitID) / count(DISTINCT o.offer_id)) * 100 as `Процент покрытия каталога` FROM ecom.ecom_offers AS o LEFT JOIN ecom.raw_events AS e ON o.offer_id = e.ContentUnitID" }]
    }
  ],
  "schemaVersion": 42
};

createDash("clickhouse-technical-ops", "ClickHouse: Технический мониторинг", techSpec);
createDash("clickhouse-business-analytics", "E-commerce: Бизнес-аналитика", businessSpec);