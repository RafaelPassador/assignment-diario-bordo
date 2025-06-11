"""
Unit tests for the DataReader class.
Tests the functionality of reading data from different sources.
"""
import pytest
from pyspark.sql import DataFrame
from src.data_tools.data_reader import DataReader


class TestDataReader:
    """Test suite for DataReader class functionality."""

    def test_read_csv_success(self, spark_session, temp_csv_file):
        """
        Test successful CSV file reading with proper headers and separator.
        Verifies that the CSV is read correctly with semicolon separator.
        """
        reader = DataReader(spark_session)
        df = reader.read_csv(temp_csv_file)
        
        assert isinstance(df, DataFrame)
        assert df.count() == 4  # 4 data rows (excluding header)
        
        # Check if columns are properly read
        expected_columns = ["data_inicio", "data_fim", "categoria", "local_inicio", "local_fim", "distancia", "proposito"]
        assert df.columns == expected_columns

    def test_read_csv_file_not_found(self, spark_session):
        """
        Test CSV reading behavior when file doesn't exist.
        Should handle the error gracefully.
        """
        reader = DataReader(spark_session)
        non_existent_file = "/path/that/does/not/exist.csv"
        
        with pytest.raises(Exception):
            reader.read_csv(non_existent_file)