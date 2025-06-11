import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
from src.data_tools.data_writer import DataWriter
from src.data_tools.data_reader import DataReader
from src.data_tools.data_processor import DataProcessor

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("TestSession")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")  # Add Delta Lake dependency
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_silver_transformation(spark):
    # Criar dados dummy para teste
    schema = StructType([
        StructField("DT_REFE", DateType(), True),
        StructField("QTD_CORRIDAS", IntegerType(), True)
    ])
    data = [
        (date(2016, 1, 1), 100),
        (date(2016, 1, 2), 200)
    ]
    dummy_df = spark.createDataFrame(data, schema)
    
    # Simular transformação para silver
    transformed_df = dummy_df.withColumnRenamed("DT_REFE", "DT_REFERENCIA")
    assert transformed_df.count() == 2
    assert "DT_REFERENCIA" in transformed_df.columns

def test_gold_aggregation(spark):
    # Criar dados dummy para teste
    schema = StructType([
        StructField("DT_REFERENCIA", DateType(), True),
        StructField("QTD_CORRIDAS", IntegerType(), True),
        StructField("TOTAL_PASSAGEIROS", IntegerType(), True)
    ])
    data = [
        (date(2016, 1, 1), 100, 150),
        (date(2016, 1, 1), 200, 300)
    ]
    dummy_df = spark.createDataFrame(data, schema)
    
    # Simular agregação para gold
    aggregated_df = dummy_df.groupBy("DT_REFERENCIA").sum("QTD_CORRIDAS", "TOTAL_PASSAGEIROS")
    row = aggregated_df.collect()[0]
    assert row["sum(QTD_CORRIDAS)"] == 300  # soma de 100 + 200
    assert row["sum(TOTAL_PASSAGEIROS)"] == 450  # soma de 150 + 300

def test_data_reader_csv(spark, tmp_path):
    # Criar dados dummy para teste
    test_data = "DT_REFERENCIA,QTD_CORRIDAS,TOTAL_PASSAGEIROS\n2016-01-01,100,150"
    test_file = tmp_path / "test.csv"
    test_file.write_text(test_data)
    
    # Testar leitura do CSV
    reader = DataReader(spark)
    df = reader.read_csv(str(test_file), header=True)  # Ensure header parsing
    assert df.count() == 1
    assert len(df.columns) == 3

def test_data_writer_delta(spark, tmp_path):
    # Criar DataFrame dummy para teste
    schema = StructType([
        StructField("DT_REFE", DateType(), True),
        StructField("QTD_CORRIDAS", IntegerType(), True)
    ])
    data = [(date(2016, 1, 1), 100)]
    dummy_df = spark.createDataFrame(data, schema)
    
    # Testar escrita Delta
    writer = DataWriter()
    test_path = str(tmp_path / "test_delta")
    writer.write_delta(dummy_df, test_path, "test_table")
    
    # Verificar se os dados foram escritos
    result_df = spark.read.format("delta").load(test_path)
    assert result_df.count() == 1

def test_invalid_data_transformation(spark):
    # Criar dados dummy inválidos
    schema = StructType([
        StructField("INVALID_COLUMN", StringType(), True)
    ])
    invalid_df = spark.createDataFrame([], schema)
    
    # Simular transformação inválida
    with pytest.raises(Exception):
        invalid_df.withColumnRenamed("INVALID_COLUMN", "DT_REFERENCIA")

def test_partition_by_date(spark, tmp_path):
    # Criar dados dummy com múltiplas datas
    schema = StructType([
        StructField("DT_REFE", DateType(), True),
        StructField("QTD_CORRIDAS", IntegerType(), True)
    ])
    data = [
        (date(2016, 1, 1), 100),
        (date(2016, 1, 2), 200)
    ]
    dummy_df = spark.createDataFrame(data, schema)
    
    # Testar escrita particionada
    writer = DataWriter()
    test_path = str(tmp_path / "test_partitioned")
    writer.write_delta(dummy_df, test_path, "test_table", partition_cols=["DT_REFE"])
    
    # Verificar partições
    partitions = spark.read.format("delta").load(test_path).select("DT_REFE").distinct()
    assert partitions.count() == 2

def test_data_quality(spark):
    # Criar dados dummy para teste
    schema = StructType([
        StructField("DT_REFE", DateType(), True),
        StructField("QTD_CORRIDAS", IntegerType(), True)
    ])
    data = [
        (date(2016, 1, 1), 100),
        (date(2016, 1, 2), -50)  # Contagem negativa para teste
    ]
    dummy_df = spark.createDataFrame(data, schema)
    
    # Testar qualidade dos dados
    assert not dummy_df.filter(dummy_df.DT_REFE.isNull()).count(), "Não deve haver datas nulas"
    assert not dummy_df.filter(dummy_df.QTD_CORRIDAS < 0).count(), "Não deve haver contagens negativas"
