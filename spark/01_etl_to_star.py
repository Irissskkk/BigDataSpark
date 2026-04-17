from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date, lit, when, year, month, dayofmonth


spark = SparkSession.builder \
    .appName("ETL_to_star") \
    .getOrCreate()

url = "jdbc:postgresql://PG_sparklab:5432/db_postgres"
props = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver",
}

print("Spark session created")

raw = spark.read.jdbc(url=url, table="public.mock_data", properties=props)
print(f"Загружено строк: {raw.count()}")

dim_customer = raw.select(
    "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_name", "customer_pet_type", "customer_pet_breed"
).distinct()

dim_customer = raw.select(
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_breed").alias("pet_breed")
).dropna(subset=["email"])

dim_customer.write.jdbc(
    url=url, table="star.dim_customer", mode="append", properties=props)
dim_customer = spark.read.jdbc(
    url=url, table="star.dim_customer", properties=props)
print(f"dim_customer: {dim_customer.count()}")

dim_seller = raw.select(
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).distinct() \
    .select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
)

dim_seller.write.jdbc(url=url, table="star.dim_seller",
                      mode="append", properties=props)
dim_seller = spark.read.jdbc(
    url=url, table="star.dim_seller", properties=props)
print(f"dim_seller: {dim_seller.count()}")

dim_store = raw.select(
    "store_name", "store_location", "store_city",
    "store_state", "store_country", "store_phone", "store_email"
).distinct() \
    .select(
        col("store_name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
)

dim_store.write.jdbc(url=url, table="star.dim_store",
                     mode="append", properties=props)
dim_store = spark.read.jdbc(
    url=url, table="star.dim_store", properties=props)
print(f"dim_store: {dim_store.count()}")

dim_supplier = raw.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).distinct() \
    .dropna() \
    .select(
        col("supplier_name"),
        col("supplier_contact").alias("contact_person"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
)

dim_supplier.write.jdbc(
    url=url, table="star.dim_supplier", mode="append", properties=props)
dim_supplier = spark.read.jdbc(
    url=url, table="star.dim_supplier", properties=props)
print(f"dim_supplier: {dim_supplier.count()}")

dim_date = raw.select("sale_date") \
    .distinct() \
    .withColumn("full_date", to_date("sale_date", "M/d/yyyy")) \
    .withColumn("year", year("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("day", dayofmonth("full_date")) \
    .withColumn("date_key", col("year") * 10000 + col("month") * 100 + col("day")) \
    .drop("sale_date") \
    .dropna() \
    .select("date_key", "full_date", "year", "month", "day")

dim_date.write.jdbc(url=url, table="star.dim_date",
                    mode="append", properties=props)
dim_date = spark.read.jdbc(
    url=url, table="star.dim_date", properties=props)
print(f"dim_date: {dim_date.count()}")

dim_product = raw.select(
    "product_name", "product_category", "pet_category",
    "product_brand", "product_color", "product_material",
    "product_size", "product_price", "product_weight",
    "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date", "product_description"
).distinct()

dim_product = raw.select(
    col("product_name"),
    col("product_category").alias("category_name"),
    col("pet_category"),
    col("product_brand").alias("brand_name"),
    col("product_color").alias("color_name"),
    col("product_material").alias("material_name"),
    col("product_size").alias("size"),
    col("product_price").alias("price"),
    col("product_weight").alias("weight"),
    col("product_rating").alias("rating"),
    col("product_reviews").alias("reviews"),
    to_date("product_release_date", "M/d/yyyy").alias("release_date"),
    to_date("product_expiry_date", "M/d/yyyy").alias("expiry_date"),
    col("product_description").alias("description")
).dropna().distinct()

dim_product.write.jdbc(url=url, table="star.dim_product",
                       mode="append", properties=props)
dim_product = spark.read.jdbc(
    url=url, table="star.dim_product", properties=props)
print(f"dim_product: {dim_product.count()}")

fact_sales = raw

fact_sales = fact_sales.join(
    dim_customer,
    fact_sales.customer_email == dim_customer.email,
    "left"
)

fact_sales = fact_sales.join(
    dim_seller,
    fact_sales.seller_email == dim_seller.email,
    "left"
)

fact_sales = fact_sales.join(
    dim_product,
    (fact_sales.product_name == dim_product.product_name) &
    (fact_sales.product_price == dim_product.price) &
    (fact_sales.product_size == dim_product.size) &
    (fact_sales.product_weight == dim_product.weight),
    "left"
)

fact_sales = fact_sales.join(
    dim_store,
    (fact_sales.store_name == dim_store.store_name) &
    (fact_sales.store_location == dim_store.location) &
    (fact_sales.store_city == dim_store.city) &
    (fact_sales.store_country == dim_store.country),
    "left"
)

fact_sales = fact_sales.join(
    dim_supplier,
    fact_sales.supplier_email == dim_supplier.email,
    "left"
)

fact_sales = fact_sales.join(
    dim_date,
    to_date(fact_sales.sale_date, "M/d/yyyy") == dim_date.full_date,
    "left"
)

fact_sales = fact_sales.select(
    col("customer_id").alias("customer_key"),
    col("seller_id").alias("seller_key"),
    col("product_id").alias("product_key"),
    col("store_key"),
    col("supplier_key"),
    col("date_key"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
)

fact_sales.write.jdbc(url=url, table="star.fact_sales",
                      mode="append", properties=props)
fact_sales = spark.read.jdbc(
    url=url, table="star.fact_sales", properties=props)
print(f"fact_sales: {fact_sales.count()}")

spark.stop()
