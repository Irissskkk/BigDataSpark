CREATE SCHEMA IF NOT EXISTS star;

-- Таблица клиентов
CREATE TABLE star.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    pet_name VARCHAR(100),
    pet_type VARCHAR(50),
    pet_breed VARCHAR(100)
);

-- Таблица продавцов
CREATE TABLE star.dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

-- Таблица продуктов
CREATE TABLE star.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category_name VARCHAR(100),
    pet_category VARCHAR(50),
    brand_name VARCHAR(100),
    color_name VARCHAR(50),
    material_name VARCHAR(100),
    size VARCHAR(50),
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    rating DECIMAL(3,2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    description TEXT
);

-- Таблица магазинов
CREATE TABLE star.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name VARCHAR(200),
    location VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200)
);

-- Таблица поставщиков
CREATE TABLE star.dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200),
    contact_person VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

-- Таблица дат
CREATE TABLE star.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- Факт-таблица продаж
CREATE TABLE star.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES star.dim_customer(customer_id),
    seller_key INTEGER REFERENCES star.dim_seller(seller_id),
    product_key INTEGER REFERENCES star.dim_product(product_id),
    store_key INTEGER REFERENCES star.dim_store(store_key),
    supplier_key INTEGER REFERENCES star.dim_supplier(supplier_key),
    date_key INTEGER REFERENCES star.dim_date(date_key),
    quantity INTEGER,
    total_price DECIMAL(10,2)
);