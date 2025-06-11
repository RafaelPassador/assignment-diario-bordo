import pytest
from decimal import Decimal
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from datetime import datetime, date, timedelta
from src.data_tools.data_writer import DataWriter
from src.data_tools.data_reader import DataReader
from src.data_tools.data_processor import DataProcessor
from src.data_tools.utils.exceptions import DataValidationError, DataProcessingError

# Initialize Faker with fixed seed for reproducibility
fake = Faker()
Faker.seed(12345)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (SparkSession.builder
            .appName("TestInfoTransportes")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.warehouse.dir", "/app/spark-warehouse")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")
            .config("spark.sql.sources.default", "delta")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("javax.jdo.option.ConnectionURL", "jdbc:derby:/app/derby/metastore_db;create=true")
            .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
            .config("javax.jdo.option.ConnectionUserName", "APP")
            .config("javax.jdo.option.ConnectionPassword", "mine")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate())
    
    # Create test database
    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    spark.sql("USE test")
    
    yield spark
    
    # Cleanup after all tests
    spark.sql("DROP DATABASE IF EXISTS test CASCADE")
    spark.stop()

def generate_test_data(num_records=3):
    """Generate random test data that matches our schema."""
    categories = ["Negocio", "Pessoal", "NEGOCIO", "PESSOAL"]
    purposes = ["Reunião", "Lazer", "REUNIÃO", "LAZER", "Entrega", "ENTREGA"]
    
    data = []
    base_date = date(2016, 1, 1)
    
    for _ in range(num_records):
        # Generate random datetime within 2016
        random_date = base_date + timedelta(days=fake.random_int(min=0, max=365))
        time_str = fake.time(pattern="%H:%M")
        data_inicio = random_date.strftime("%d-%m-%Y") + " " + time_str
        
        # Generate other fields
        categoria = fake.random_element(categories)
        distancia = Decimal(str(round(fake.pyfloat(min_value=1, max_value=100, right_digits=2), 2)))
        proposito = fake.random_element(purposes)
        
        data.append((data_inicio, categoria, distancia, proposito))
    
    return data

@pytest.fixture(scope="function")
def sample_bronze_data(spark):
    """Create sample data for bronze layer testing."""
    schema = StructType([
        StructField("data_inicio", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("distancia", DecimalType(17,2), True),
        StructField("proposito", StringType(), True)
    ])
    
    data = generate_test_data()
    df = spark.createDataFrame(data, schema)
    
    # Write test data to a temporary table
    df.write.format("delta").mode("overwrite").saveAsTable("test.bronze_test_data")
    
    yield df
    
    # Cleanup after each test
    spark.sql("DROP TABLE IF EXISTS test.bronze_test_data")

@pytest.fixture
def sample_silver_data(spark):
    """Create sample data for silver layer testing."""
    schema = StructType([
        StructField("dt_refe", DateType(), True),
        StructField("categoria", StringType(), True),
        StructField("distancia", DecimalType(17,2), True),
        StructField("proposito", StringType(), True)
    ])
    
    # Generate data with already transformed values
    data = []
    for _ in range(3):
        dt_refe = fake.date_between(start_date=date(2016, 1, 1), end_date=date(2016, 12, 31))
        categoria = fake.random_element(["negocio", "pessoal"])  # Already lowercase
        distancia = Decimal(str(round(fake.pyfloat(min_value=1, max_value=100, right_digits=2), 2)))
        proposito = fake.random_element(["reunião", "lazer", "entrega"])  # Already lowercase
        
        data.append((dt_refe, categoria, distancia, proposito))
    
    return spark.createDataFrame(data, schema)

def test_bronze_processor_validation(spark, sample_bronze_data, tmp_path):
    """Test bronze layer data validation."""
    reader = DataReader(spark)
    writer = DataWriter()
    processor = DataProcessor(spark, reader, writer)
    
    # Test with valid data
    result_df = processor.processors['bronze'].process(target_table="test_bronze")
    assert result_df.count() == sample_bronze_data.count()
    assert all(col in result_df.columns for col in ["data_inicio", "categoria", "distancia", "proposito"])
    
    # Test with invalid data (missing required column)
    invalid_df = sample_bronze_data.drop("categoria")
    with pytest.raises(DataValidationError):
        processor.processors['bronze'].process(target_table="test_bronze_invalid")

def test_silver_processor_transformations(spark, sample_silver_data, tmp_path):
    """Test silver layer transformations."""
    reader = DataReader(spark)
    writer = DataWriter()
    processor = DataProcessor(spark, reader, writer)
    
    result_df = processor.processors['silver'].process(
        source_table="test_bronze",
        target_table="test_silver"
    )
    
    # Test date transformation
    assert "dt_refe" in result_df.columns
    
    # Test lowercase transformations
    sample_row = result_df.first()
    assert sample_row.categoria.islower()
    assert sample_row.proposito.islower()
    
    # Test null validation
    assert result_df.filter(result_df.dt_refe.isNull()).count() == 0
    assert result_df.filter(result_df.distancia.isNull()).count() == 0

def test_bronze_transformations(spark):
    """Test transformations in the Bronze layer."""
    from src.data_tools.utils.transformations import LowerCaseTransformation

    # Create dummy data
    schema = StructType([
        StructField("data_inicio", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("distancia", DecimalType(17, 2), True),
        StructField("proposito", StringType(), True)
    ])

    data = [
        ("01-01-2016 12:00", "Negocio", Decimal("10.5"), "Reunião"),
        ("02-01-2016 13:00", "Pessoal", Decimal("20.0"), "Lazer")
    ]

    df = spark.createDataFrame(data, schema)

    # Apply transformations
    transformation = LowerCaseTransformation("categoria")
    transformed_df = transformation.apply(df)

    # Validate transformations
    assert transformed_df.filter(transformed_df.categoria == "negocio").count() == 1
    assert transformed_df.filter(transformed_df.categoria == "pessoal").count() == 1

def test_silver_transformations(spark):
    """Test transformations in the Silver layer."""
    from src.data_tools.utils.transformations import DateTransformation, LowerCaseTransformation

    # Create dummy data
    schema = StructType([
        StructField("data_inicio", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("distancia", DecimalType(17, 2), True),
        StructField("proposito", StringType(), True)
    ])

    data = [
        ("01-01-2016 12:00", "Negocio", Decimal("10.5"), "Reunião"),
        ("02-01-2016 13:00", "Pessoal", Decimal("20.0"), "Lazer")
    ]

    df = spark.createDataFrame(data, schema)

    # Apply transformations
    transformations = [
        DateTransformation("data_inicio", "dt_refe", "dd-MM-yyyy HH:mm"),
        LowerCaseTransformation("categoria"),
        LowerCaseTransformation("proposito")
    ]

    for transformation in transformations:
        df = transformation.apply(df)

    # Validate transformations
    assert "dt_refe" in df.columns
    assert df.filter(df.categoria == "negocio").count() == 1
    assert df.filter(df.proposito == "reunião").count() == 1

def test_gold_aggregations(spark):
    """Test aggregations in the Gold layer."""
    from pyspark.sql.functions import count, max, avg

    # Create dummy data
    schema = StructType([
        StructField("dt_refe", DateType(), True),
        StructField("categoria", StringType(), True),
        StructField("distancia", DecimalType(17, 2), True),
        StructField("proposito", StringType(), True)
    ])

    data = [
        (date(2016, 1, 1), "negocio", Decimal("10.5"), "reunião"),
        (date(2016, 1, 1), "pessoal", Decimal("20.0"), "lazer"),
        (date(2016, 1, 2), "negocio", Decimal("15.0"), "entrega")
    ]

    df = spark.createDataFrame(data, schema)

    # Perform aggregations
    aggregated_df = df.groupBy("dt_refe").agg(
        count("*").alias("qt_corr"),
        max("distancia").alias("vl_max_dist"),
        avg("distancia").alias("vl_avg_dist")
    )

    # Validate aggregations
    assert aggregated_df.filter(aggregated_df.dt_refe == date(2016, 1, 1)).count() == 1
    assert aggregated_df.filter(aggregated_df.vl_max_dist == Decimal("20.0")).count() == 1
    assert aggregated_df.filter(aggregated_df.vl_avg_dist == Decimal("15.25")).count() == 1

def test_error_handling(spark):
    """Test error handling in the processors."""
    reader = DataReader(spark)
    writer = DataWriter()
    processor = DataProcessor(spark, reader, writer)
    
    # Test with non-existent table
    with pytest.raises(DataProcessingError):
        processor.transform_silver(
            source_table="non_existent_table",
            target_table="test_silver"
        )
