"""
Test configuration and fixtures for the data pipeline tests.
This module provides common test configuration and reusable fixtures.
"""
import pytest
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a test Spark session configured for unit testing.
    Uses local mode and memory storage to avoid external dependencies.
    """
    spark = (
        SparkSession.builder
        .appName("UnitTests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )
    
    # Set log level to reduce noise during testing
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_csv_data():
    """
    Provides sample CSV data that mimics the structure of the input file.
    This data can be used to test the pipeline without relying on actual files.
    """
    return [
        "data_inicio;data_fim;categoria;local_inicio;local_fim;distancia;proposito",
        "01-01-2016 21:11;01-01-2016 21:17;Negocio;Fort Pierce;Fort Pierce;51;Alimentação",
        "01-02-2016 01:25;01-02-2016 01:37;Negocio;Fort Pierce;Fort Pierce;5;",
        "01-02-2016 20:25;01-02-2016 20:38;Pessoal;Fort Pierce;Fort Pierce;48;Entregas",
        "01-05-2016 17:31;01-05-2016 17:45;Negocio;Fort Pierce;Fort Pierce;47;Reunião"
    ]


@pytest.fixture
def temp_csv_file(sample_csv_data):
    """
    Creates a temporary CSV file with sample data for testing.
    Returns the path to the temporary file and cleans up after use.
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        for line in sample_csv_data:
            f.write(line + '\n')
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    try:
        os.unlink(temp_path)
    except OSError:
        pass


@pytest.fixture
def temp_dir():
    """
    Creates a temporary directory for testing file operations.
    Returns the path and cleans up after use.
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    
    # Cleanup would normally be here, but we'll let the OS handle it
    # to avoid issues with Spark still accessing files


@pytest.fixture
def sample_schema():
    """
    Provides the expected schema for the input data.
    This helps ensure data consistency across tests.
    """
    return StructType([
        StructField("data_inicio", StringType(), True),
        StructField("data_fim", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("local_inicio", StringType(), True),
        StructField("local_fim", StringType(), True),
        StructField("distancia", StringType(), True),
        StructField("proposito", StringType(), True)
    ])
