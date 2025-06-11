import logging
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.data_tools.data_processor import DataProcessor
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter

sys.path.append('/app/src')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        builder = (
            SparkSession.builder
            .appName("InfoTransportesPipeline")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        logging.info("Iniciando pipeline InfoTransportes...")

        reader = DataReader(spark)
        writer = DataWriter()
        processor = DataProcessor(spark, reader, writer)
        processor.load_bronze()
        processor.transform_silver()
        processor.aggregate_gold().show()

        logging.info("Pipeline finalizado com sucesso.")
        spark.stop()
    except Exception as e:
        logging.critical(f"Falha crítica na execução da pipeline: {e}")
        raise

if __name__ == "__main__":
    main()