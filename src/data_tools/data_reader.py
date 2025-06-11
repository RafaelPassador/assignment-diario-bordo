import logging
from pyspark.sql import SparkSession

class DataReader:
    """
    Class responsible for reading data from various sources.

    Methods:
        - read_csv: Reads data from a CSV file.
        - read_delta: Reads data from a Delta table.
        - read_parquet: Reads data from a Parquet file.
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
        return self.spark.read.option("header", True).option("sep", ";").csv(path)

    def read_delta(self, path, table_name=None):
        """
        Reads data from a Delta table.

        Args:
            path (str): Path to the Delta table.
            table_name (str, optional): Nome da tabela registrada no catálogo.
                                      Se fornecido, tentará ler a tabela pelo nome primeiro.

        Returns:
            DataFrame: Spark DataFrame containing the Delta data.
        """
        if table_name:
            try:
                logging.info(f"Tentando ler tabela '{table_name}' do catálogo")
                return self.spark.table(table_name)
            except Exception as e:
                logging.warning(f"Não foi possível ler a tabela '{table_name}' do catálogo: {e}")
                logging.info(f"Tentando ler pelo caminho: {path}")
        
        logging.info(f"Lendo Delta de {path}")
        return self.spark.read.format("delta").load(path)

    def read_parquet(self, path):
        """
        Reads data from a Parquet file.

        Args:
            path (str): Path to the Parquet file.

        Returns:
            DataFrame: Spark DataFrame containing the Parquet data.
        """
        logging.info(f"Lendo Parquet de {path}")
        return self.spark.read.parquet(path)