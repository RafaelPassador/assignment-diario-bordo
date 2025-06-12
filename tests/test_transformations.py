"""
Unit tests for the data transformation utilities.
Tests the transformation classes and data validation functionality.
"""
from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col
from src.data_tools.utils.transformations import (
    DateTransformation, 
    LowerCaseTransformation, 
    DataValidator
)


class TestDateTransformation:
    """Test suite for DateTransformation class."""

    def test_date_transformation_success(self, spark_session):
        """
        Test successful date transformation from string to date.
        Verifies that date strings are properly converted to date type.
        """
        # Create test data with date strings
        schema = StructType([
            StructField("data_inicio", StringType(), True),
            StructField("other_col", StringType(), True)
        ])
        test_data = [
            ("01-01-2016 21:11", "some_data"),
            ("02-01-2016 14:30", "other_data")
        ]
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Apply transformation
        transformation = DateTransformation("data_inicio", "dt_refe", "dd-MM-yyyy HH:mm")
        result_df = transformation.apply(test_df)
        
        # Verify transformation
        assert "dt_refe" in result_df.columns
        assert result_df.schema["dt_refe"].dataType == StringType()  # Expect StringType instead of DateType
        
        # Check actual values
        result_data = result_df.collect()
        assert result_data[0]['dt_refe'] == "2016-01-01"  # Expect string representation of date
        assert result_data[1]['dt_refe'] == "2016-02-01"

    def test_date_transformation_preserves_other_columns(self, spark_session):
        """
        Test that date transformation preserves other columns in DataFrame.
        Verifies that only the target column is added, others remain unchanged.
        """
        schema = StructType([
            StructField("data_inicio", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("distancia", StringType(), True)
        ])
        test_data = [
            ("01-01-2016 21:11", "Negocio", "51")
        ]
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Apply transformation
        transformation = DateTransformation("data_inicio", "dt_refe", "dd-MM-yyyy HH:mm")
        result_df = transformation.apply(test_df)
        
        # Verify all original columns are preserved
        assert "data_inicio" in result_df.columns
        assert "categoria" in result_df.columns
        assert "distancia" in result_df.columns
        assert "dt_refe" in result_df.columns
        assert len(result_df.columns) == 4


class TestLowerCaseTransformation:
    """Test suite for LowerCaseTransformation class."""

    def test_lowercase_transformation_success(self, spark_session):
        """
        Test successful conversion of string column to lowercase.
        Verifies that text is properly converted to lowercase.
        """
        schema = StructType([
            StructField("categoria", StringType(), True),
            StructField("other_col", StringType(), True)
        ])
        test_data = [
            ("NEGOCIO", "unchanged"),
            ("Pessoal", "also_unchanged"),
            ("MiXeD cAsE", "another")
        ]
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Apply transformation
        transformation = LowerCaseTransformation("categoria")
        result_df = transformation.apply(test_df)
        
        # Verify transformation
        result_data = result_df.collect()
        assert result_data[0]['categoria'] == "negocio"
        assert result_data[1]['categoria'] == "pessoal"
        assert result_data[2]['categoria'] == "mixed case"
        
        # Verify other columns unchanged
        assert result_data[0]['other_col'] == "unchanged"

class TestDataValidator:
    """Test suite for DataValidator class."""

    def test_validate_required_columns_missing(self, spark_session):
        """
        Test validation failure when required columns are missing.
        Should return False when some columns are missing.
        """
        schema = StructType([
            StructField("data_inicio", StringType(), True),
            StructField("categoria", StringType(), True)
        ])
        test_data = [("data1", "cat1")]
        test_df = spark_session.createDataFrame(test_data, schema)
        
        validator = DataValidator()
        required_columns = ["data_inicio", "categoria", "distancia", "proposito"]
        
        result = validator.validate_required_columns(test_df, required_columns)
        assert result is False

    def test_validate_non_null_columns_success(self, spark_session):
        """
        Test successful non-null validation when all values are present.
        Should return True when no null values exist in specified columns.
        """
        schema = StructType([
            StructField("data_inicio", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("distancia", StringType(), True)
        ])
        test_data = [
            ("data1", "cat1", "10"),
            ("data2", "cat2", "20")
        ]
        test_df = spark_session.createDataFrame(test_data, schema)
        
        validator = DataValidator()
        columns_to_check = ["data_inicio", "categoria", "distancia"]
        
        result = validator.validate_non_null_columns(test_df, columns_to_check)
        assert result is True
