"""
Unit tests for the DataWriter class.
Tests the functionality of writing data to different formats.
"""
import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from src.data_tools.data_writer import DataWriter


class TestDataWriter:
    """Test suite for DataWriter class functionality."""

    def test_write_parquet_success(self, spark_session, temp_dir, sample_schema):
        """
        Test successful writing of DataFrame to Parquet format.
        Verifies that data is written correctly and can be read back.
        """
        writer = DataWriter()
        
        # Create test data
        test_data = [
            ("01-01-2016 21:11", "01-01-2016 21:17", "Negocio", "Fort Pierce", "Fort Pierce", "51", "Alimentação"),
            ("01-02-2016 01:25", "01-02-2016 01:37", "Pessoal", "Fort Pierce", "Fort Pierce", "5", "")
        ]
        test_df = spark_session.createDataFrame(test_data, sample_schema)
        parquet_path = os.path.join(temp_dir, "test_output.parquet")
        
        # Write parquet file
        writer.write_parquet(test_df, parquet_path)
        
        # Verify file was created and can be read
        assert os.path.exists(parquet_path)
        read_df = spark_session.read.parquet(parquet_path)
        assert read_df.count() == 2
        assert read_df.columns == test_df.columns