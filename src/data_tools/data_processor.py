import logging
from pyspark.sql.functions import to_date, col, lower, count, avg, min, max, when
from src.config import INPUT_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter
import os
class DataProcessor:
    """
    Class responsible for processing data in Bronze, Silver, and Gold layers.

    Methods:
        - load_bronze: Loads data from the input layer to the Bronze layer.
        - transform_silver: Transforms data from the Bronze layer to the Silver layer.
        - aggregate_gold: Aggregates data from the Silver layer to the Gold layer.

    """
    def __init__(self, spark, reader: DataReader, writer: DataWriter):
        """
        Initializes the DataProcessor with the required dependencies.

        Args:
            spark (SparkSession): Spark instance.
            reader (DataReader): Object responsible for reading data.
            writer (DataWriter): Object responsible for writing data.
        """
        self.reader = reader
        self.writer = writer

    def load_bronze(self, table_name: str = "bronze_table"):
        """
        Loads data from the input layer (CSV) to the Bronze layer (Delta).

        Args:
            table_name (str): Nome da tabela a ser criada no catálogo.

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_csv(INPUT_PATH)
            logging.info("Csv lido com sucesso.")
            self.writer.write_delta(df, BRONZE_PATH, table_name)
            logging.info("Carga bronze concluída com sucesso.")
            #printh table path
            logging.info(f"Dados carregados no caminho: {BRONZE_PATH}")
        except Exception as e:
            logging.error(f"Erro ao carregar bronze: {e}")
            raise

    def transform_silver(self, source_table: str = "bronze_table", target_table: str = "silver_table"):
        """
        Transforms data from the Bronze layer to the Silver layer, applying
        formatting and filtering.

        Args:
            source_table (str): Nome da tabela bronze de origem.
            target_table (str): Nome da tabela silver a ser criada.

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_delta(BRONZE_PATH, table_name=source_table)
            df = df.withColumn("DT_REFE", to_date(col("DATA_INICIO"), "dd-MM-yyyy HH:mm"))
            df = df.withColumn("CATEGORIA", lower(col("CATEGORIA")))
            df = df.withColumn("PROPOSITO", lower(col("PROPOSITO")))
            df = df.dropna(subset=["DT_REFE", "CATEGORIA", "DISTANCIA"])
            self.writer.write_delta(df, SILVER_PATH, target_table, partition_cols=["DT_REFE"])
            logging.info("Transformação silver concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro na transformação silver: {e}")
            raise

    def aggregate_gold(self, source_table: str = "silver_table", target_table: str = "gold_table"):
        """
        Aggregates data from the Silver layer to the Gold layer, calculating
        metrics such as count, average, maximum, and minimum.

        Args:
            source_table (str): Nome da tabela silver de origem.
            target_table (str): Nome da tabela gold a ser criada.

        Returns:
            DataFrame: Resulting DataFrame from the aggregation.

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_delta(SILVER_PATH, table_name=source_table)
            grouped = df.groupBy("DT_REFE").agg(
                count("*").alias("QT_CORR"),
                count(when(col("CATEGORIA") == "negocio", True)).alias("QT_CORR_NEG"),
                count(when(col("CATEGORIA") == "pessoal", True)).alias("QT_CORR_PESS"),
                max(col("DISTANCIA")).alias("VL_MAX_DIST"),
                min(col("DISTANCIA")).alias("VL_MIN_DIST"),
                avg(col("DISTANCIA")).alias("VL_AVG_DIST"),
                count(when(col("PROPOSITO") == "reunião", True)).alias("QT_CORR_REUNI"),
                count(when((col("PROPOSITO") != "reunião") & col("PROPOSITO").isNotNull(), True)).alias("QT_CORR_NAO_REUNI")
            )
            self.writer.write_delta(grouped, GOLD_PATH, target_table, partition_cols=["DT_REFE"])
            logging.info("Agregação gold concluída com sucesso.")
            return grouped
        except Exception as e:
            logging.error(f"Erro na agregação gold: {e}")
            raise