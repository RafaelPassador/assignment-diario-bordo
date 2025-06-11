import logging
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, when, col, avg, min, max

from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter
from src.data_tools.utils.config import DataConfig
from src.data_tools.utils.exceptions import DataProcessingError, DataValidationError
from src.data_tools.utils.transformations import DataValidator, DateTransformation, LowerCaseTransformation

class BaseProcessor(ABC):
    def __init__(self, reader: DataReader, writer: DataWriter, config: DataConfig):
        self.reader = reader
        self.writer = writer
        self.config = config
        self.validator = DataValidator()

    @abstractmethod
    def process(self, source_table: str, target_table: str) -> DataFrame:
        pass

class BronzeProcessor(BaseProcessor):
    def process(self, source_table: str = None, target_table: str = "bronze_table") -> DataFrame:
        try:
            df = self.reader.read_csv(self.config.input_path)
            logging.info("CSV read successfully")
            
            # Convert column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])
            
            # Validate input data
            required_columns = ["data_inicio", "categoria", "distancia", "proposito"]
            if not self.validator.validate_required_columns(df, required_columns):
                raise DataValidationError("Missing required columns in input data")

            self.writer.write_delta(df, self.config.bronze_path, target_table)
            logging.info(f"Bronze load completed successfully. Data loaded at: {self.config.bronze_path}")
            return df
            
        except Exception as e:
            logging.error(f"Error loading bronze: {e}")
            raise DataProcessingError(f"Bronze processing failed: {str(e)}")

class SilverProcessor(BaseProcessor):
    def process(self, source_table: str = "bronze_table", target_table: str = "silver_table") -> DataFrame:
        try:
            df = self.reader.read_delta(self.config.bronze_path, table_name=source_table)
            
            # Apply transformations
            transformations = [
                DateTransformation("data_inicio", "dt_refe", "dd-MM-yyyy HH:mm"),
                LowerCaseTransformation("categoria"),
                LowerCaseTransformation("proposito")
            ]
            
            for transformation in transformations:
                df = transformation.apply(df)

            # Validate transformed data
            required_columns = ["dt_refe", "categoria", "distancia"]
            if not self.validator.validate_non_null_columns(df, required_columns):
                df = df.dropna(subset=required_columns)
            
            self.writer.write_delta(df, self.config.silver_path, target_table, partition_cols=["dt_refe"])
            logging.info("Silver transformation completed successfully")
            return df
            
        except Exception as e:
            logging.error(f"Error in silver transformation: {e}")
            raise DataProcessingError(f"Silver processing failed: {str(e)}")

class GoldProcessor(BaseProcessor):
    def process(self, source_table: str = "silver_table", target_table: str = "gold_table") -> DataFrame:
        try:
            from pyspark.sql.types import DecimalType, IntegerType
            from pyspark.sql.functions import col
            
            df = self.reader.read_delta(self.config.silver_path, table_name=source_table)
            
            # Perform aggregations with proper column names and data types
            grouped = df.groupBy("dt_refe").agg(
                count("*").cast(IntegerType()).alias("qt_corr"),
                count(when(col("categoria") == "negocio", True)).cast(IntegerType()).alias("qt_corr_neg"),
                count(when(col("categoria") == "pessoal", True)).cast(IntegerType()).alias("qt_corr_pess"),
                max(col("distancia")).cast(DecimalType(17,2)).alias("vl_max_dist"),
                min(col("distancia")).cast(DecimalType(17,2)).alias("vl_min_dist"),
                avg(col("distancia")).cast(DecimalType(17,2)).alias("vl_avg_dist"),
                count(when(col("proposito") == "reunião", True)).cast(IntegerType()).alias("qt_corr_reuni"),
                count(when((col("proposito") != "reunião") & col("proposito").isNotNull(), True))
                    .cast(IntegerType()).alias("qt_corr_nao_reuni")
            )
            
            self.writer.write_delta(grouped, self.config.gold_path, target_table, partition_cols=["dt_refe"])
            logging.info("Gold aggregation completed successfully")
            return grouped
            
        except Exception as e:
            logging.error(f"Error in gold aggregation: {e}")
            raise DataProcessingError(f"Gold processing failed: {str(e)}")

class ProcessorFactory:
    @staticmethod
    def create_processor(layer: str, reader: DataReader, writer: DataWriter, config: DataConfig) -> BaseProcessor:
        processors = {
            'bronze': BronzeProcessor,
            'silver': SilverProcessor,
            'gold': GoldProcessor
        }
        processor_class = processors.get(layer.lower())
        if not processor_class:
            raise ValueError(f"Invalid layer: {layer}")
        return processor_class(reader, writer, config)
