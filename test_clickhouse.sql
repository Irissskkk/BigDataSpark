-- Скрипт для полного тестирования всех VIEW по заданию
-- Запускать в ClickHouse (http://localhost:8124 или через DBeaver)

USE reports;

-- === rep1: Витрина продаж по продуктам ===

echo '=== 1.1. Топ-10 самых продаваемых продуктов ===';
SELECT * FROM top10_products_by_quantity;

echo '=== 1.2. Общая выручка по категориям продуктов ===';
SELECT * FROM revenue_by_category;

echo '=== 1.3. Средний рейтинг и количество отзывов для каждого продукта ===';
SELECT * FROM product_rating_reviews;

-- === rep2: Витрина продаж по клиентам ===

echo '=== 2.1. Топ-10 клиентов с наибольшей общей суммой покупок ===';
SELECT * FROM top10_customers_by_spent;

echo '=== 2.2. Распределение клиентов по странам ===';
SELECT * FROM customers_by_country;

echo '=== 2.3. Средний чек для каждого клиента ===';
SELECT * FROM avg_check_per_customer;

-- === rep3: Витрина продаж по времени ===

echo '=== 3.1. Месячные тренды продаж ===';
SELECT * FROM sales_trends_month;

echo '=== 3.2. Годовые тренды продаж ===';
SELECT * FROM sales_trends_year;

echo '=== 3.3. Средний размер заказа по месяцам ===';
SELECT * FROM avg_order_by_month;

-- === rep4: Витрина продаж по магазинам ===

echo '=== 4.1. Топ-5 магазинов с наибольшей выручкой ===';
SELECT * FROM top5_stores_by_revenue;

echo '=== 4.2. Распределение продаж по городам и странам ===';
SELECT * FROM sales_by_city;

echo '=== 4.3. Распределение продаж по странам ===';
SELECT * FROM sales_by_country;

echo '=== 4.4. Средний чек для каждого магазина ===';
SELECT * FROM avg_check_per_store;

-- === rep5: Витрина продаж по поставщикам ===

echo '=== 5.1. Топ-5 поставщиков с наибольшей выручкой ===';
SELECT * FROM top5_suppliers_by_revenue;

echo '=== 5.2. Средняя цена товаров от каждого поставщика ===';
SELECT * FROM avg_price_per_supplier;

echo '=== 5.3. Распределение продаж по странам поставщиков ===';
SELECT * FROM sales_by_supplier_country;

-- === rep6: Витрина качества продукции ===

echo '=== 6.1. Продукты с наивысшим рейтингом ===';
SELECT * FROM top_rated_products;

echo '=== 6.2. Продукты с наименьшим рейтингом ===';
SELECT * FROM lowest_rated_products;

echo '=== 6.3. Корреляция между рейтингом и объемом продаж ===';
SELECT * FROM rating_sales_correlation;

echo '=== 6.4. Продукты с наибольшим количеством отзывов ===';
SELECT * FROM top_products_by_reviews;

-- Проверка количества записей в каждой таблице
echo '=== Проверка количества записей в репозиториях ===';
SELECT 'rep1' as table_name, count() as row_count FROM rep1
UNION ALL SELECT 'rep2', count() FROM rep2
UNION ALL SELECT 'rep3', count() FROM rep3
UNION ALL SELECT 'rep4', count() FROM rep4
UNION ALL SELECT 'rep5', count() FROM rep5
UNION ALL SELECT 'rep6', count() FROM rep6;
