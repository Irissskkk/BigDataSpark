# BigDataSpark - ETL лабораторная работа

Реализация ETL-пайплайна с помощью Apache Spark: трансформация данных из CSV файлов в Star Schema в PostgreSQL, создание аналитических витрин в ClickHouse.

# Архитектура проекта

```
CSV файлы (10 шт.), PostgreSQL (mock_data), Spark ETL, PostgreSQL (Star Schema), Spark Reports, ClickHouse (20 VIEW)
```

# Структура проекта

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

**Linux:**
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```

# Старт

```bash
# Запустить все контейнеры
docker-compose up -d

# Смотреть логи 
docker logs -f spark-submit

# Проверить результаты
./test_all.sh
```

# Запуск контейнеров

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

# Шаг 2: Мониторинг выполнения

```bash
# Смотреть логи spark-submit 
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

# Шаг 3: Проверка баз данных

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

# Проверка результатов

# Автоматическое тестирование

```bash
# Запустить автоматический тестовый скрипт
chmod +x test_all.sh
./test_all.sh

# Скрипт проверит:
# - PostgreSQL: исходные данные и Star Schema
# - ClickHouse: все 20 VIEW
# - Покажет примеры данных
```

# Через SQL клиент (DBeaver)

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
USE reports;
SHOW TABLES;
-- Примеры запросов
SELECT * FROM top10_products_by_quantity;
SELECT * FROM top10_customers_by_spent;
SELECT * FROM sales_trends_month;
```

# Через терминал

```bash
# PostgreSQL
docker exec -it PG_sparklab psql -U admin -d db_postgres -c "SELECT COUNT(*) FROM star.fact_sales"

# ClickHouse (через curl)
curl "http://localhost:8124/" --user admin:password --data "SELECT * FROM reports.top10_products_by_quantity FORMAT Pretty"
```
