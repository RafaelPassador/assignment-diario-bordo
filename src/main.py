import logging
import sys
from pyspark.sql import SparkSession
from src.data_tools.data_processor import DataProcessor
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter
from src.config import BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE
from src.data_tools.utils.exceptions import DataProcessingError

sys.path.append('/app/src')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def create_spark_session() -> SparkSession:
    """Creates and configures a Spark session."""
    builder = (
        SparkSession.builder
        .appName("InfoTransportesPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.warehouse.dir", "/app/spark-warehouse")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.session.timeZone", "UTC")
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:/app/derby/metastore_db;create=true")
        .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .config("javax.jdo.option.ConnectionUserName", "APP")
        .config("javax.jdo.option.ConnectionPassword", "mine")
        .enableHiveSupport()
    )
    return builder.getOrCreate()

def setup_database(spark: SparkSession) -> None:
    """Sets up the database configuration."""
    spark.sql("SET spark.sql.legacy.createHiveTableByDefault = false")
    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    spark.sql("USE default")

def run_pipeline(spark: SparkSession) -> None:
    """Executes the data processing pipeline."""
    try:
        logging.info("Iniciando pipeline InfoTransportes...")
        
        # Log table names
        logging.info("Usando as tabelas:")
        logging.info(f"Bronze: {BRONZE_TABLE}")
        logging.info(f"Silver: {SILVER_TABLE}")
        logging.info(f"Gold: {GOLD_TABLE}")

        # Initialize components
        reader = DataReader(spark)
        writer = DataWriter()
        processor = DataProcessor(spark, reader, writer)
        
        # Execute pipeline
        processor.load_bronze(table_name=BRONZE_TABLE)
        processor.transform_silver(source_table=BRONZE_TABLE, target_table=SILVER_TABLE)
        processor.aggregate_gold(source_table=SILVER_TABLE, target_table=GOLD_TABLE)

        logging.info("Pipeline finalizado com sucesso.")
    
    except DataProcessingError as e:
        logging.error(f"Erro durante o processamento dos dados: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado: {str(e)}")
        raise
    
def main():
    """Main entry point of the application."""
    spark = None
    try:
        spark = create_spark_session()
        setup_database(spark)
        run_pipeline(spark)
    except Exception as e:
        logging.error(f"Pipeline falhou: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session encerrada.")

if __name__ == "__main__":
    main()