from pyspark.sql import SparkSession
from src.config import BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def test_tables():
    # Inicializa o Spark
    spark = SparkSession.builder \
        .appName("TestQueries") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Mostra todas as tabelas
        logging.info("\nTabelas disponíveis:")
        spark.sql("SHOW TABLES").show()

        # Testa cada tabela
        for table in [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE]:
            logging.info(f"\nEstrutura da tabela {table}:")
            spark.sql(f"DESCRIBE EXTENDED {table}").show(truncate=False)
            
            logging.info(f"\nAmostra de dados da tabela {table}:")
            spark.sql(f"SELECT * FROM {table} LIMIT 5").show()

            # Conta registros
            count = spark.sql(f"SELECT COUNT(*) as total FROM {table}").collect()[0]['total']
            logging.info(f"Total de registros em {table}: {count}")

    except Exception as e:
        logging.error(f"Erro ao testar tabelas: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    test_tables()
