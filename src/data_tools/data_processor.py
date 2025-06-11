import logging
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter
from src.data_tools.utils.config import DataConfig
from src.data_tools.utils.processors import ProcessorFactory, BaseProcessor

class DataProcessor:
    """
    Class responsible for orchestrating data processing across Bronze, Silver, and Gold layers.
    This class follows the Single Responsibility Principle by delegating actual processing
    to specialized processor classes.
    """
    def __init__(self, spark: SparkSession, reader: DataReader, writer: DataWriter):
        """
        Initializes the DataProcessor with the required dependencies.

        Args:
            spark (SparkSession): Spark instance.
            reader (DataReader): Object responsible for reading data.
            writer (DataWriter): Object responsible for writing data.
        """
        self.config = DataConfig()
        self.processors: Dict[str, BaseProcessor] = {
            'bronze': ProcessorFactory.create_processor('bronze', reader, writer, self.config),
            'silver': ProcessorFactory.create_processor('silver', reader, writer, self.config),
            'gold': ProcessorFactory.create_processor('gold', reader, writer, self.config)
        }

    def load_bronze(self, table_name: str = "bronze_table") -> DataFrame:
        """
        Loads data from the input layer (CSV) to the Bronze layer (Delta).

        Args:
            table_name (str): Name of the table to be created in the catalog.

        Returns:
            DataFrame: The processed DataFrame.

        Raises:
            DataProcessingError: If an error occurs during the process.
        """
        return self.processors['bronze'].process(target_table=table_name)

    def transform_silver(self, source_table: str = "bronze_table", target_table: str = "silver_table") -> DataFrame:
        """
        Transforms data from the Bronze layer to the Silver layer.

        Args:
            source_table (str): Name of the source bronze table.
            target_table (str): Name of the target silver table.

        Returns:
            DataFrame: The processed DataFrame.

        Raises:
            DataProcessingError: If an error occurs during the process.
        """
        return self.processors['silver'].process(source_table, target_table)

    def aggregate_gold(self, source_table: str = "silver_table", target_table: str = "gold_table") -> DataFrame:
        """
        Aggregates data from the Silver layer to the Gold layer.

        Args:
            source_table (str): Name of the source silver table.
            target_table (str): Name of the target gold table.

        Returns:
            DataFrame: The processed DataFrame.

        Raises:
            DataProcessingError: If an error occurs during the process.
        """
        return self.processors['gold'].process(source_table, target_table)