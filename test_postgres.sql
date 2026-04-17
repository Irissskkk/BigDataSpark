-- Скрипт для тестирования PostgreSQL Star Schema
-- Запускать в PostgreSQL (DBeaver или psql)

\c db_postgres

-- Проверка исходных данных
echo '=== Проверка исходных данных ===';
SELECT COUNT(*) as mock_data_count FROM public.mock_data;

-- Проверка размерных таблиц
echo '=== Проверка размерных таблиц ===';
SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM star.dim_customer
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM star.dim_seller
UNION ALL
SELECT 'dim_product', COUNT(*) FROM star.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM star.dim_store
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM star.dim_supplier
UNION ALL
SELECT 'dim_date', COUNT(*) FROM star.dim_date;

-- Проверка факт-таблицы
echo '=== Проверка факт-таблицы ===';
SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM star.fact_sales;

-- Примеры выборок для проверки качества данных
echo '=== Пример данных из dim_customer ===';
SELECT * FROM star.dim_customer LIMIT 5;

echo '=== Пример данных из dim_product ===';
SELECT product_name, category_name, price, rating FROM star.dim_product LIMIT 5;

echo '=== Пример данных из fact_sales ===';
SELECT * FROM star.fact_sales LIMIT 5;

-- Проверка связей
echo '=== Проверка связей (продажи с_null внешними ключами) ===';
SELECT
    COUNT(*) FILTER (WHERE customer_key IS NULL) as null_customer,
    COUNT(*) FILTER (WHERE seller_key IS NULL) as null_seller,
    COUNT(*) FILTER (WHERE product_key IS NULL) as null_product,
    COUNT(*) FILTER (WHERE store_key IS NULL) as null_store,
    COUNT(*) FILTER (WHERE supplier_key IS NULL) as null_supplier,
    COUNT(*) FILTER (WHERE date_key IS NULL) as null_date
FROM star.fact_sales;

-- Агрегация для проверки
echo '=== Общая выручка по факт-таблице ===';
SELECT SUM(total_price) as total_revenue, SUM(quantity) as total_quantity FROM star.fact_sales;

echo '=== Топ-5 продуктов по выручке (через Star Schema) ===';
SELECT
    p.product_name,
    p.category_name,
    SUM(f.quantity) as total_sold,
    SUM(f.total_price) as revenue
FROM star.fact_sales f
JOIN star.dim_product p ON f.product_key = p.product_id
GROUP BY p.product_id, p.product_name, p.category_name
ORDER BY revenue DESC
LIMIT 5;

echo '=== Топ-5 клиентов по тратам (через Star Schema) ===';
SELECT
    c.first_name,
    c.last_name,
    c.email,
    SUM(f.total_price) as total_spent
FROM star.fact_sales f
JOIN star.dim_customer c ON f.customer_key = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
ORDER BY total_spent DESC
LIMIT 5;
