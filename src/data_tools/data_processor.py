import logging
from pyspark.sql.functions import to_date, col, lower, count, avg, min, max, when
from src.config import INPUT_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter

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

    def load_bronze(self):
        """
        Loads data from the input layer (CSV) to the Bronze layer (Delta).

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_csv(INPUT_PATH)
            self.writer.write_delta(df, BRONZE_PATH)
            logging.info("Carga bronze concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao carregar bronze: {e}")
            raise

    def transform_silver(self):
        """
        Transforms data from the Bronze layer to the Silver layer, applying
        formatting and filtering.

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_delta(BRONZE_PATH)
            df = df.withColumn("DT_REFE", to_date(col("DATA_INICIO"), "MM-dd-yyyy HH"))
            df = df.withColumn("CATEGORIA", lower(col("CATEGORIA")))
            df = df.withColumn("PROPOSITO", lower(col("PROPOSITO")))
            df = df.dropna(subset=["DT_REFE", "CATEGORIA", "DISTANCIA"])
            self.writer.write_delta(df, SILVER_PATH, partition_cols=["DT_REFE"])
            logging.info("Transformação silver concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro na transformação silver: {e}")
            raise

    def aggregate_gold(self):
        """
        Aggregates data from the Silver layer to the Gold layer, calculating
        metrics such as count, average, maximum, and minimum.

        Returns:
            DataFrame: Resulting DataFrame from the aggregation.

        Raises:
            Exception: If an error occurs during the process.
        """
        try:
            df = self.reader.read_delta(SILVER_PATH)
            grouped = df.groupBy("DT_REFE").agg(
                count("*").alias("QT_CORR"),
                count(when(col("CATEGORIA") == "negócio", True)).alias("QT_CORR_NEG"),
                count(when(col("CATEGORIA") == "pessoal", True)).alias("QT_CORR_PESS"),
                max(col("DISTANCIA")).alias("VL_MAX_DIST"),
                min(col("DISTANCIA")).alias("VL_MIN_DIST"),
                avg(col("DISTANCIA")).alias("VL_AVG_DIST"),
                count(when(col("PROPOSITO") == "reunião", True)).alias("QT_CORR_REUNI"),
                count(when((col("PROPOSITO") != "reunião") & col("PROPOSITO").isNotNull(), True)).alias("QT_CORR_NAO_REUNI")
            )
            self.writer.write_delta(grouped, GOLD_PATH, partition_cols=["DT_REFE"])
            logging.info("Agregação gold concluída com sucesso.")
            return grouped
        except Exception as e:
            logging.error(f"Erro na agregação gold: {e}")
            raise