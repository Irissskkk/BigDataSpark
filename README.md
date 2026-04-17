# BigDataSpark - ETL лабораторная работа

Реализация ETL-пайплайна с помощью Apache Spark: трансформация данных из CSV файлов в Star Schema в PostgreSQL, создание аналитических витрин в ClickHouse.

## 📋 Содержание

- [Архитектура проекта](#архитектура-проекта)
- [Структура проекта](#структура-проекта)
- [Требования](#требования)
- [Быстрый старт](#быстрый-старт)
- [Подробная инструкция](#подробная-инструкция)
- [Проверка результатов](#проверка-результатов)
- [Витрины данных](#витрины-данных)
- [Интерфейсы для работы с БД](#интерфейсы-для-работы-с-бд)
- [ Troubleshooting](#troubleshooting)

## 🏗️ Архитектура проекта

```
CSV файлы (10 шт.) → PostgreSQL (mock_data) → Spark ETL → PostgreSQL (Star Schema) → Spark Reports → ClickHouse (20 VIEW)
```

## 📁 Структура проекта

```
├── docker-compose.yaml          # Docker конфигурация
├── data/                        # Исходные CSV файлы
│   ├── mock_data_0.csv ... mock_data_9.csv
├── sql_init/                    # SQL скрипты для инициализации
│   ├── 01-create-mock.sql      # Создание таблицы mock_data
│   ├── 02-import-csv.sql       # Импорт CSV файлов
│   └── 03-create-star.sql      # Создание Star Schema
├── spark/                       # Spark скрипты
│   ├── 01_etl_to_star.py       # ETL: mock_data → Star Schema
│   ├── 02_reports_to_clickhouse.py  # Star Schema → ClickHouse VIEWs
│   └── run.sh                  # Скрипт запуска Spark jobs
├── test_all.sh                  # Автоматическое тестирование
├── test_clickhouse.sql          # SQL скрипты для тестирования ClickHouse
└── test_postgres.sql            # SQL скрипты для тестирования PostgreSQL
```

## ⚙️ Требования

- **Docker** & **Docker Compose** (обязательно)
- **DBeaver** или другой SQL клиент (рекомендуется)

### Установка Docker

**Windows:**
```bash
# Скачать Docker Desktop: https://www.docker.com/products/docker-desktop
```

**Linux:**
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```

## 🚀 Быстрый старт

```bash
# 1. Запустить все контейнеры
docker-compose up -d

# 2. Смотреть логи (ждать "All tasks done!")
docker logs -f spark-submit

# 3. Проверить результаты
./test_all.sh
```

**⏱️ Время выполнения:** ~3-5 минут (первый запуск дольше из-за скачивания JDBC драйверов)

## 📖 Подробная инструкция

### Шаг 1: Запуск контейнеров

```bash
# Запустить PostgreSQL, ClickHouse, Spark Master, Spark Worker, Spark Submit
docker-compose up -d

# Проверить статус контейнеров
docker ps

# Должны быть активны:
# - PG_sparklab (PostgreSQL)
# - db_clickhouse (ClickHouse)
# - spark-master
# - spark-worker
# - spark-submit
```

### Шаг 2: Мониторинг выполнения

```bash
# Смотреть логи spark-submit (где выполняются ETL процессы)
docker logs -f spark-submit

# Ожидаемые выводы:
# 1. "Waiting for PostgreSQL and ClickHouse..."
# 2. "Downloading PostgreSQL JDBC driver..."
# 3. "Downloading ClickHouse JDBC driver..."
# 4. "Running ETL to Star Schema..."
# 5. "ETL to Star Schema completed!"
# 6. "Running Reports to ClickHouse..."
# 7. "All tasks done!"
```

### Шаг 3: Проверка баз данных

#### PostgreSQL
- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `db_postgres`
- **User:** `admin`
- **Password:** `admin123`

#### ClickHouse
- **Host:** `localhost`
- **Port:** `8124` (HTTP) или `9001` (Native)
- **Database:** `reports`
- **User:** `admin`
- **Password:** `password`

## ✅ Проверка результатов

### Способ 1: Автоматическое тестирование

```bash
# Запустить автоматический тестовый скрипт
chmod +x test_all.sh
./test_all.sh

# Скрипт проверит:
# - PostgreSQL: исходные данные и Star Schema
# - ClickHouse: все 20 VIEW
# - Покажет примеры данных
```

### Способ 2: Через SQL клиент (DBeaver)

#### PostgreSQL проверка:
```sql
-- Исходные данные
SELECT COUNT(*) FROM public.mock_data; -- Должно быть: 10000

-- Star Schema таблицы
SELECT COUNT(*) FROM star.dim_customer;
SELECT COUNT(*) FROM star.dim_seller;
SELECT COUNT(*) FROM star.dim_product;
SELECT COUNT(*) FROM star.dim_store;
SELECT COUNT(*) FROM star.dim_supplier;
SELECT COUNT(*) FROM star.dim_date;
SELECT COUNT(*) FROM star.fact_sales;
```

#### ClickHouse проверка:
```sql
-- Использовать базу reports
USE reports;

-- Проверить наличие всех VIEW
SHOW TABLES;

-- Примеры запросов
SELECT * FROM top10_products_by_quantity;
SELECT * FROM top10_customers_by_spent;
SELECT * FROM sales_trends_month;
```

### Способ 3: Через терминал

```bash
# PostgreSQL
docker exec -it PG_sparklab psql -U admin -d db_postgres -c "SELECT COUNT(*) FROM star.fact_sales"

# ClickHouse (через curl)
curl "http://localhost:8124/" --user admin:password --data "SELECT * FROM reports.top10_products_by_quantity FORMAT Pretty"
```

## 📊 Витрины данных

### **rep1: Витрина продаж по продуктам**
- `top10_products_by_quantity` — Топ-10 самых продаваемых продуктов
- `revenue_by_category` — Общая выручка по категориям
- `product_rating_reviews` — Средний рейтинг и отзывы

### **rep2: Витрина продаж по клиентам**
- `top10_customers_by_spent` — Топ-10 клиентов по тратам
- `customers_by_country` — Распределение клиентов по странам
- `avg_check_per_customer` — Средний чек по клиентам

### **rep3: Витрина продаж по времени**
- `sales_trends_month` — Месячные тренды продаж
- `sales_trends_year` — Годовые тренды продаж
- `avg_order_by_month` — Средний размер заказа по месяцам

### **rep4: Витрина продаж по магазинам**
- `top5_stores_by_revenue` — Топ-5 магазинов по выручке
- `sales_by_city` — Продажи по городам
- `sales_by_country` — Продажи по странам
- `avg_check_per_store` — Средний чек по магазинам

### **rep5: Витрина продаж по поставщикам**
- `top5_suppliers_by_revenue` — Топ-5 поставщиков по выручке
- `avg_price_per_supplier` — Средняя цена по поставщикам
- `sales_by_supplier_country` — Продажи по странам поставщиков

### **rep6: Витрина качества продукции**
- `top_rated_products` — Продукты с наивысшим рейтингом
- `lowest_rated_products` — Продукты с наименьшим рейтингом
- `rating_sales_correlation` — Корреляция рейтинг-продажи
- `top_products_by_reviews` — Топ продуктов по отзывам

## 🎨 Интерфейсы для работы с БД

### DBeaver (Рекомендуется!)

**Почему DBeaver:**
- 🆓 Бесплатная версия полная функциональность
- 🔌 Поддержка PostgreSQL и ClickHouse
- 📊 Визуализация данных, экспорт в Excel
- 🔍 Автодополнение SQL запросов
- 📈 Построение графиков

**Установка:**
```bash
# Скачать: https://dbeaver.io/download/
```

**Подключение PostgreSQL:**
```
Host: localhost
Port: 5432
Database: db_postgres
Username: admin
Password: admin123
```

**Подключение ClickHouse:**
```
Host: localhost
Port: 8124
Database: reports
Username: admin
Password: password
Driver: ClickHouse (автоскачивание)
```

### Альтернативные интерфейсы

**Tabix (веб-интерфейс для ClickHouse):**
```bash
docker run -d -p 8080:8080 \
  -e CLICKHOUSE_URL="http://host.docker.internal:8124" \
  -e CLICKHOUSE_USER="admin" \
  -e CLICKHOUSE_PASSWORD="password" \
  spoonest/tabix

# Открыть: http://localhost:8080
```

**DataGrip (платный):**
- Отличная IDE от JetBrains
- 30 дней бесплатного триала

## 🔧 Troubleshooting

### Проблема: Контейнеры не стартуют

```bash
# Проверить логи конкретного контейнера
docker logs PG_sparklab
docker logs db_clickhouse
docker logs spark-submit

# Перезапустить всё
docker-compose down
docker-compose up -d
```

### Проблема: Spark не может подключиться к БД

```bash
# Проверить сеть
docker network ls
docker network inspect bigdataspark_br-net

# Проверить доступность БД из spark контейнера
docker exec spark-submit ping PG_sparklab
docker exec spark-submit ping db_clickhouse
```

### Проблема: JDBC драйвера не скачиваются

```bash
# Вручную скачать и положить в jars/
curl -L -o jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
curl -L -o jars/clickhouse-jdbc-0.4.6-all.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar
```

### Проблема: Данные не импортировались

```bash
# Проверить наличие CSV файлов
ls -la data/

# Проверить выполнение SQL скриптов
docker logs PG_sparklab | grep "mock_data"

# Вручную запустить импорт
docker exec -i PG_sparklab psql -U admin -d db_postgres -f /docker-entrypoint-initdb.d/02-import-csv.sql
```

### Проблема: VIEW в ClickHouse не создаются

```bash
# Проверить логи spark-submit
docker logs spark-submit | grep "VIEW"

# Вручную выполнить SQL из тестового скрипта
docker exec -i db_clickhouse clickhouse-client --user admin --password password --database reports --multiquery < test_clickhouse.sql
```

## 📝 Полезные команды

```bash
# Остановить все контейнеры
docker-compose down

# Остановить и удалить volumes (очистка данных)
docker-compose down -v

# Пересобрать образы
docker-compose build

# Перезапустить spark-submit
docker restart spark-submit

# Подключиться к PostgreSQL
docker exec -it PG_sparklab psql -U admin -d db_postgres

# Подключиться к ClickHouse
docker exec -it db_clickhouse clickhouse-client --user admin --password password
```

## 🎯 Результат работы

После успешного выполнения вы получите:

1. ✅ **PostgreSQL** с исходными данными (10,000 записей)
2. ✅ **PostgreSQL Star Schema** (6 размерных таблиц + 1 факт-таблица)
3. ✅ **ClickHouse** с 20 аналитическими VIEW
4. ✅ Полный ETL-пайплайн на Apache Spark

## 📚 Дополнительные материалы

- [Документация Apache Spark](https://spark.apache.org/docs/latest/)
- [Документация ClickHouse](https://clickhouse.com/docs)
- [Документация PostgreSQL](https://www.postgresql.org/docs/)
