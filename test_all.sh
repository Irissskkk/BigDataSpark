#!/bin/bash

echo "  Скрипт для полного тестирования"

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для выполнения запросов к ClickHouse
query_ch() {
    local sql="$1"
    curl -s "http://localhost:8124/" --user admin:password --data-binary "$sql" 2>/dev/null
}

# Функция для выполнения запросов к PostgreSQL
query_pg() {
    local sql="$1"
    docker exec -i PG_sparklab psql -U admin -d db_postgres -c "$sql" 2>/dev/null
}

echo -e "\n${YELLOW}1. Проверка PostgreSQL${NC}"
echo "=========================================="

echo -e "\n${YELLOW}1.1. Проверка исходных данных${NC}"
count=$(query_pg "SELECT COUNT(*) FROM public.mock_data" | grep -o '[0-9]*' | head -1)
if [ "$count" -eq "10000" ]; then
    echo -e "${GREEN}✓ mock_data: $count записей${NC}"
else
    echo -e "${RED}✗ mock_data: $count записей (ожидается 10000)${NC}"
fi

echo -e "\n${YELLOW}1.2. Проверка Star Schema таблиц${NC}"
tables=("dim_customer" "dim_seller" "dim_product" "dim_store" "dim_supplier" "dim_date" "fact_sales")
for table in "${tables[@]}"; do
    count=$(query_pg "SELECT COUNT(*) FROM star.$table" | grep -o '[0-9]*' | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $table: $count записей${NC}"
    else
        echo -e "${RED}✗ $table: ошибка или 0 записей${NC}"
    fi
done

echo -e "\n${YELLOW}2. Проверка ClickHouse${NC}"
echo "=========================================="

# Проверяем существует ли база данных
db_check=$(query_ch "EXISTS DATABASE reports")
if [ "$db_check" = "1" ]; then
    echo -e "${GREEN}✓ База данных 'reports' существует${NC}"
else
    echo -e "${RED}✗ База данных 'reports' не существует${NC}"
    exit 1
fi

echo -e "\n${YELLOW}2.1. Проверка репозиториев${NC}"
reps=("rep1" "rep2" "rep3" "rep4" "rep5" "rep6")
for rep in "${reps[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$rep" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $rep: $count записей${NC}"
    else
        echo -e "${RED}✗ $rep: ошибка или 0 записей${NC}"
    fi
done

echo -e "\n${YELLOW}2.2. Проверка всех VIEW (20 штук)${NC}"

# rep1 views
echo -e "\n${YELLOW}--- rep1: Витрина продаж по продуктам ---${NC}"
views=(
    "top10_products_by_quantity"
    "revenue_by_category"
    "product_rating_reviews"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка или 0 записей${NC}"
    fi
done

# rep2 views
echo -e "\n${YELLOW}--- rep2: Витрина продаж по клиентам ---${NC}"
views=(
    "top10_customers_by_spent"
    "customers_by_country"
    "avg_check_per_customer"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка или 0 записей${NC}"
    fi
done

# rep3 views
echo -e "\n${YELLOW}--- rep3: Витрина продаж по времени ---${NC}"
views=(
    "sales_trends_month"
    "sales_trends_year"
    "avg_order_by_month"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка или 0 записей${NC}"
    fi
done

# rep4 views
echo -e "\n${YELLOW}--- rep4: Витрина продаж по магазинам ---${NC}"
views=(
    "top5_stores_by_revenue"
    "sales_by_city"
    "sales_by_country"
    "avg_check_per_store"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка или 0 записей${NC}"
    fi
done

# rep5 views
echo -e "\n${YELLOW}--- rep5: Витрина продаж по поставщикам ---${NC}"
views=(
    "top5_suppliers_by_revenue"
    "avg_price_per_supplier"
    "sales_by_supplier_country"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ] && [ "$count" -gt "0" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка или 0 записей${NC}"
    fi
done

# rep6 views
echo -e "\n${YELLOW}--- rep6: Витрина качества продукции ---${NC}"
views=(
    "top_rated_products"
    "lowest_rated_products"
    "rating_sales_correlation"
    "top_products_by_reviews"
)
for view in "${views[@]}"; do
    count=$(query_ch "SELECT count() FROM reports.$view" | head -1)
    if [ -n "$count" ]; then
        echo -e "${GREEN}✓ $view: $count записей${NC}"
    else
        echo -e "${RED}✗ $view: ошибка${NC}"
    fi
done

echo -e "\n${YELLOW}3. Демонстрация данных из VIEW${NC}"
echo "=========================================="

echo -e "\n${YELLOW}Топ-3 продукта по количеству:${NC}"
query_ch "SELECT * FROM reports.top10_products_by_quantity LIMIT 3 FORMAT Pretty"

echo -e "\n${YELLOW}Топ-3 клиента по тратам:${NC}"
query_ch "SELECT * FROM reports.top10_customers_by_spent LIMIT 3 FORMAT Pretty"

echo -e "\n${YELLOW}Тренды продаж по месяцам:${NC}"
query_ch "SELECT * FROM reports.sales_trends_month ORDER BY year, month LIMIT 5 FORMAT Pretty"

echo -e "\n=========================================="
echo -e "${GREEN}Тестирование завершено!${NC}"
echo "=========================================="
