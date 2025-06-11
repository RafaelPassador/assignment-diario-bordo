import logging
import sys
from pyspark.sql import SparkSession
from src.data_tools.data_processor import DataProcessor
from src.data_tools.data_reader import DataReader
from src.data_tools.data_writer import DataWriter
from src.config import BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE

sys.path.append('/app/src')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # Configuração do SparkSession
        builder = (
            SparkSession.builder
            .appName("InfoTransportesPipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.warehouse.dir", "spark-warehouse")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")
            .config("spark.sql.sources.default", "delta")
            .config("spark.sql.session.timeZone", "UTC")
            .enableHiveSupport()
        )

        spark = builder.getOrCreate()
        
        # Configura o catálogo
        spark.sql("SET spark.sql.legacy.createHiveTableByDefault = false")
        spark.sql("CREATE DATABASE IF NOT EXISTS default")
        spark.sql("USE default")

        logging.info("Iniciando pipeline InfoTransportes...")
        
        # Log dos nomes das tabelas que serão usadas
        logging.info("Usando as tabelas:")
        logging.info(f"Bronze: {BRONZE_TABLE}")
        logging.info(f"Silver: {SILVER_TABLE}")
        logging.info(f"Gold: {GOLD_TABLE}")

        reader = DataReader(spark)
        writer = DataWriter()
        processor = DataProcessor(spark, reader, writer)
        
        # Executa o pipeline com os nomes das tabelas das configurações
        processor.load_bronze(table_name=BRONZE_TABLE)
        processor.transform_silver(source_table=BRONZE_TABLE, target_table=SILVER_TABLE)
        result_df = processor.aggregate_gold(source_table=SILVER_TABLE, target_table=GOLD_TABLE)
        
        # Verifica as tabelas criadas
        logging.info("Tabelas disponíveis no catálogo:")
        spark.sql("SHOW TABLES").show()
        
        # Verifica detalhes das tabelas criadas
        for table in [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE]:
            logging.info(f"\nDetalhes da tabela {table}:")
            spark.sql(f"DESCRIBE EXTENDED {table}").show(truncate=False)
            
        # Mostra uma amostra dos dados de cada tabela
        for table in [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE]:
            logging.info(f"\nAmostra de dados da tabela {table}:")
            spark.sql(f"SELECT * FROM {table} LIMIT 5").show()

        logging.info("Pipeline finalizado com sucesso.")
        spark.stop()
    except Exception as e:
        logging.critical(f"Falha crítica na execução da pipeline: {e}")
        raise

if __name__ == "__main__":
    main()