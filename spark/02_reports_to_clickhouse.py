import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, col, desc, row_number, year, month, round

spark = SparkSession.builder \
    .appName("Reports_to_ClickHouse") \
    .getOrCreate()

pg_url = "jdbc:postgresql://PG_sparklab:5432/db_postgres"
pg_props = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

ch_url = "jdbc:clickhouse://clickhouse:8123/reports"
ch_http_url = "http://clickhouse:8123/"
ch_props = {
    "user": "admin",
    "password": "password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "http_connection_provider": "HTTP_URL_CONNECTION",
    "createTableOptions": "ENGINE = MergeTree() ORDER BY tuple()"
}

fact_sales = spark.read.jdbc(
    url=pg_url, table="star.fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(
    url=pg_url, table="star.dim_product", properties=pg_props)
dim_customer = spark.read.jdbc(
    url=pg_url, table="star.dim_customer", properties=pg_props)
dim_store = spark.read.jdbc(
    url=pg_url, table="star.dim_store", properties=pg_props)
dim_supplier = spark.read.jdbc(
    url=pg_url, table="star.dim_supplier", properties=pg_props)
dim_date = spark.read.jdbc(
    url=pg_url, table="star.dim_date", properties=pg_props)

print(f"load fact_sales: {fact_sales.count()}")
print(f"load dim_product: {dim_product.count()}")
print(f"load dim_customer: {dim_customer.count()}")
print(f"load dim_store: {dim_store.count()}")
print(f"load dim_supplier: {dim_supplier.count()}")
print(f"load dim_date: {dim_date.count()}")


def http_req(sql: str):
    try:
        req = urllib.request.Request(ch_http_url, data=sql.encode('utf-8'))
        req.add_header('X-ClickHouse-User', 'admin')
        req.add_header('X-ClickHouse-Key', 'password')
        urllib.request.urlopen(req)
        words = sql.split()
        if len(words) > 5 and words[3] == 'VIEW':
            print(f"VIEW {words[4]} created")
    except Exception as e:
        print(e)


http_req("CREATE DATABASE IF NOT EXISTS reports;")

rep1 = fact_sales.join(dim_product, fact_sales.product_key == dim_product.product_id, "left") \
    .select("product_id", "product_name", "category_name", "rating", "reviews", "quantity", "total_price")
rep1.write.jdbc(url=ch_url, table="rep1",
                mode="overwrite", properties=ch_props)

rep1 = spark.read.jdbc(url=ch_url, table="rep1", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.top10_products_by_quantity AS
    SELECT 
        product_id,
        product_name,
        category_name,
        SUM(quantity) AS total_quantity_sold,
        SUM(total_price) AS total_revenue
    FROM reports.rep1
    GROUP BY product_id, product_name, category_name
    ORDER BY total_quantity_sold DESC
    LIMIT 10;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.revenue_by_category AS
    SELECT 
        category_name,
        SUM(total_price) AS total_revenue
    FROM reports.rep1
    GROUP BY category_name
    ORDER BY total_revenue DESC;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.product_rating_reviews AS
    SELECT 
        product_id,
        product_name,
        AVG(rating) AS avg_rating,
        SUM(reviews) AS total_reviews
    FROM reports.rep1
    GROUP BY product_id, product_name
    ORDER BY avg_rating DESC;
""")

rep2 = dim_customer.join(fact_sales, dim_customer.customer_id == fact_sales.customer_key, "left") \
    .groupBy("customer_id", "first_name", "last_name", "email", "country") \
    .agg(
        sum("total_price").alias("total_spent"),
        round(avg("total_price"), 2).cast("decimal(15,2)").alias("avg_check")
)

rep2.write.jdbc(url=ch_url, table="rep2",
                mode="overwrite", properties=ch_props)
rep2 = spark.read.jdbc(url=ch_url, table="rep2", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.top10_customers_by_spent AS
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        total_spent
    FROM reports.rep2
    ORDER BY total_spent DESC
    LIMIT 10;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.customers_by_country AS
    SELECT 
        country,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM reports.rep2
    GROUP BY country
    ORDER BY customer_count DESC;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.avg_check_per_customer AS
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        avg_check
    FROM reports.rep2
    ORDER BY avg_check DESC;
""")

rep3 = fact_sales.join(dim_date, "date_key") \
    .groupBy("year", "month") \
    .agg(
        sum("total_price").alias("revenue"),
        round(avg("total_price"), 2).cast(
            "decimal(15,2)").alias("avg_order_price"),
        count("*").alias("orders_count"),
        sum("quantity").alias("total_quantity"),
        round(avg("quantity"), 2).cast("decimal(15,2)").alias("avg_quantity"),
)

rep3.write.jdbc(url=ch_url, table="rep3",
                mode="overwrite", properties=ch_props)
rep3 = spark.read.jdbc(url=ch_url, table="rep3", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.sales_trends_month AS
    SELECT 
        year,
        month,
        revenue,
        avg_order_price,
        orders_count,
        total_quantity
    FROM reports.rep3
    ORDER BY year, month;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.sales_trends_year AS
    SELECT 
        year,
        SUM(revenue) AS revenue,
        ROUND(AVG(avg_order_price),2) AS avg_order_price,
        SUM(orders_count) AS orders_count,
        SUM(total_quantity) AS total_quantity
    FROM reports.rep3
    GROUP BY year
    ORDER BY year;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.avg_order_by_month AS
    SELECT 
        year,
        month,
        avg_order_price,
        avg_quantity
    FROM reports.rep3
    ORDER BY year, month;
""")

rep4 = fact_sales.join(dim_store, "store_key") \
    .groupBy("store_key", "store_name", "city", "country") \
    .agg(
        sum("total_price").alias("revenue"),
        round(avg("total_price"), 2).cast("decimal(15,2)").alias("avg_check"),
        count("*").alias("sales_count")
)
rep4.write.jdbc(url=ch_url, table="rep4",
                mode="overwrite", properties=ch_props)
rep4 = spark.read.jdbc(url=ch_url, table="rep4", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.top5_stores_by_revenue AS
    SELECT
        store_key,
        store_name,
        revenue
    FROM reports.rep4
    ORDER BY revenue DESC
    LIMIT 5;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.sales_by_city AS
    SELECT
        country,
        city,
        SUM(revenue) AS total_revenue
    FROM reports.rep4
    GROUP BY country, city
    ORDER BY total_revenue DESC;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.sales_by_country AS
    SELECT
        country,
        SUM(revenue) AS total_revenue
    FROM reports.rep4
    GROUP BY country
    ORDER BY total_revenue DESC;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.avg_check_per_store AS
    SELECT
        store_key,
        store_name,
        avg_check
    FROM reports.rep4
    ORDER BY avg_check DESC;
""")

rep5 = fact_sales.join(dim_supplier, "supplier_key") \
    .join(dim_product, dim_product.product_id == fact_sales.product_key) \
    .groupBy("supplier_key", "supplier_name", "country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        round(avg("price"), 2).cast("decimal(15,2)").alias("avg_price"),
        count("*").alias("sales_count")
)

rep5.write.jdbc(url=ch_url, table="rep5",
                mode="overwrite", properties=ch_props)
rep5 = spark.read.jdbc(url=ch_url, table="rep5", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.top5_suppliers_by_revenue AS
    SELECT 
        supplier_key,
        supplier_name,
        total_revenue
    FROM reports.rep5
    ORDER BY total_revenue DESC
    LIMIT 5;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.avg_price_per_supplier AS
    SELECT
        supplier_key,
        supplier_name,
        avg_price
    FROM reports.rep5
    ORDER BY avg_price DESC;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.sales_by_supplier_country AS
    SELECT 
        country,
        SUM(total_revenue) AS total_revenue,
        COUNT(DISTINCT supplier_key) AS supplier_count,
        SUM(sales_count) as sales_count
    FROM reports.rep5
    GROUP BY country
    ORDER BY total_revenue DESC;
""")

rep6 = fact_sales.join(dim_product, fact_sales.product_key == dim_product.product_id).groupBy("product_id", "product_name", "rating", "reviews") \
    .agg(
        sum("quantity").alias("total_sold"),
)
rep6.write.jdbc(url=ch_url, table="rep6",
                mode="overwrite", properties=ch_props)
rep6 = spark.read.jdbc(url=ch_url, table="rep6", properties=ch_props)

http_req("""
    CREATE OR REPLACE VIEW reports.top_rated_products AS
    SELECT
        product_id,
        product_name,
        rating,
        total_sold,
        reviews
    FROM reports.rep6
    WHERE rating = (SELECT MAX(rating) FROM reports.rep6)
    ORDER BY product_name;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.lowest_rated_products AS
    SELECT 
        product_id,
        product_name,
        rating,
        total_sold,
        reviews
    FROM reports.rep6
    WHERE rating = (SELECT MIN(rating) FROM reports.rep6)
    ORDER BY product_name;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.rating_sales_correlation AS
    SELECT 
        corr(toFloat64(rating), toFloat64(total_sold)) AS correlation
    FROM reports.rep6;
""")

http_req("""
    CREATE OR REPLACE VIEW reports.top_products_by_reviews AS
    SELECT
        product_id,
        product_name,
        reviews,
        rating,
        total_sold
    FROM reports.rep6
    ORDER BY reviews DESC, rating DESC
    LIMIT 10;
""")

print("\n=== top10_products_by_quantity ===")
spark.read.jdbc(url=ch_url, table="top10_products_by_quantity",
                properties=ch_props).show(truncate=False)

print("\n=== revenue_by_category ===")
spark.read.jdbc(url=ch_url, table="revenue_by_category",
                properties=ch_props).show(truncate=False)

print("\n=== product_rating_reviews ===")
spark.read.jdbc(url=ch_url, table="product_rating_reviews",
                properties=ch_props).show(truncate=False)

print("\n=== top10_customers_by_spent ===")
spark.read.jdbc(url=ch_url, table="top10_customers_by_spent",
                properties=ch_props).show(truncate=False)

print("\n=== customers_by_country ===")
spark.read.jdbc(url=ch_url, table="customers_by_country",
                properties=ch_props).show(truncate=False)

print("\n=== avg_check_per_customer ===")
spark.read.jdbc(url=ch_url, table="avg_check_per_customer",
                properties=ch_props).show(truncate=False)

print("\n=== sales_trends_month ===")
spark.read.jdbc(url=ch_url, table="sales_trends_month",
                properties=ch_props).show(truncate=False)

print("\n=== sales_trends_year ===")
spark.read.jdbc(url=ch_url, table="sales_trends_year",
                properties=ch_props).show(truncate=False)

print("\n=== avg_order_by_month ===")
spark.read.jdbc(url=ch_url, table="avg_order_by_month",
                properties=ch_props).show(truncate=False)

print("\n=== top5_stores_by_revenue ===")
spark.read.jdbc(url=ch_url, table="top5_stores_by_revenue",
                properties=ch_props).show(truncate=False)

print("\n=== sales_by_city ===")
spark.read.jdbc(url=ch_url, table="sales_by_city",
                properties=ch_props).show(truncate=False)

print("\n=== sales_by_country ===")
spark.read.jdbc(url=ch_url, table="sales_by_country",
                properties=ch_props).show(truncate=False)

print("\n=== avg_check_per_store ===")
spark.read.jdbc(url=ch_url, table="avg_check_per_store",
                properties=ch_props).show(truncate=False)

print("\n=== top5_suppliers_by_revenue ===")
spark.read.jdbc(url=ch_url, table="top5_suppliers_by_revenue",
                properties=ch_props).show(truncate=False)

print("\n=== avg_price_per_supplier ===")
spark.read.jdbc(url=ch_url, table="avg_price_per_supplier",
                properties=ch_props).show(truncate=False)

print("\n=== sales_by_supplier_country ===")
spark.read.jdbc(url=ch_url, table="sales_by_supplier_country",
                properties=ch_props).show(truncate=False)

print("\n=== top_rated_products ===")
spark.read.jdbc(url=ch_url, table="top_rated_products",
                properties=ch_props).show(truncate=False)

print("\n=== lowest_rated_products ===")
spark.read.jdbc(url=ch_url, table="lowest_rated_products",
                properties=ch_props).show(truncate=False)

print("\n=== rating_sales_correlation ===")
spark.read.jdbc(url=ch_url, table="rating_sales_correlation",
                properties=ch_props).show(truncate=False)

print("\n=== top_products_by_reviews ===")
spark.read.jdbc(url=ch_url, table="top_products_by_reviews",
                properties=ch_props).show(truncate=False)

spark.stop()
