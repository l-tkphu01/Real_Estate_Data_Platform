import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Tạo SparkSession dùng chung cho toàn bộ pytest session để tăng tốc quá trình test."""
    spark_session = SparkSession.builder \
        .appName("Pytest-PySpark-Unit-Tests") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()
