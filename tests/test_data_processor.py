import pytest
from pyspark.sql import SparkSession
from src.data_tools.data_writer import DataWriter
from src.data_tools.data_reader import DataReader
from src.data_tools.data_processor import DataProcessor
from src.config import SILVER_PATH

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("TestSession")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_silver_transformation(spark, tmp_path):
    processor = DataProcessor(spark)
    processor.load_bronze()
    processor.transform_silver()
    df = DataReader(spark).read_delta(SILVER_PATH)
    assert df.count() > 0
    assert "DT_REFE" in df.columns

def test_gold_aggregation(spark):
    processor = DataProcessor(spark)
    result_df = processor.aggregate_gold()
    assert "QT_CORR" in result_df.columns
    assert result_df.count() >= 0