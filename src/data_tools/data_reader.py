import logging
from pyspark.sql import SparkSession

class DataReader:
    """
    Class responsible for reading data from various sources.

    Methods:
        - read_csv: Reads data from a CSV file.
        - read_delta: Reads data from a Delta table.
    """
    def __init__(self, spark: SparkSession):
        """
        Initializes the DataReader with a Spark session.

        Args:
            spark (SparkSession): Spark instance.
        """
        self.spark = spark

    def read_csv(self, path):
        """
        Reads data from a CSV file.

        Args:
            path (str): Path to the CSV file.

        Returns:
            DataFrame: Spark DataFrame containing the CSV data.
        """
        logging.info(f"Lendo CSV de {path}")
        return self.spark.read.option("header", True).csv(path)

    def read_delta(self, path):
        """
        Reads data from a Delta table.

        Args:
            path (str): Path to the Delta table.

        Returns:
            DataFrame: Spark DataFrame containing the Delta data.
        """
        logging.info(f"Lendo Delta de {path}")
        return self.spark.read.format("delta").load(path)