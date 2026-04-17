#!/bin/bash

echo "Waiting for PostgreSQL and ClickHouse..."
sleep 30

# Создаем отдельную директорию для JDBC драйверов
mkdir -p /opt/spark/external-jars

# Скачиваем JDBC драйверы если они еще не скачаны
if [ ! -f /opt/spark/external-jars/postgresql-*.jar ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    curl -L -o /opt/spark/external-jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
fi

if [ ! -f /opt/spark/external-jars/clickhouse-jdbc-*.jar ]; then
    echo "Downloading ClickHouse JDBC driver..."
    curl -L -o /opt/spark/external-jars/clickhouse-jdbc-0.4.6-all.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar
fi

echo "JDBC drivers downloaded successfully!"

echo "Running ETL to Star Schema..."
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /opt/spark/external-jars/postgresql-42.6.0.jar,/opt/spark/external-jars/clickhouse-jdbc-0.4.6-all.jar \
    /opt/spark/jobs/01_etl_to_star.py

if [ $? -eq 0 ]; then
    echo "✓ ETL to Star Schema completed successfully!"
else
    echo "✗ ETL to Star Schema failed!"
    exit 1
fi

echo "Running Reports to ClickHouse..."
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /opt/spark/external-jars/postgresql-42.6.0.jar,/opt/spark/external-jars/clickhouse-jdbc-0.4.6-all.jar \
    /opt/spark/jobs/02_reports_to_clickhouse.py

if [ $? -eq 0 ]; then
    echo "✓ Reports to ClickHouse completed successfully!"
else
    echo "✗ Reports to ClickHouse failed!"
    exit 1
fi

echo "✓ All tasks done!"
