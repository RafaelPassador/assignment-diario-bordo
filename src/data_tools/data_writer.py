import logging
import os
from datetime import date

class DataWriter:
    """
    Class responsible for writing data to various formats.

    Methods:
        - write_delta: Writes data to a Delta table using a base path and table name.
        - write_parquet: Writes data to a Parquet file.
    """

    def write_delta(self, df, path, table_name, mode="overwrite", partition_cols=None):
        """
        Writes data to a Delta table.

        Args:
            df (DataFrame): Spark DataFrame to be written.
            path (str): Path to the Delta table.
            table_name (str): Name of the Delta table to be created/updated.
            mode (str): Write mode (default is "overwrite").
            partition_cols (list, optional): List of columns to partition by.
        """
        spark = df.sparkSession
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        logging.info(f"Saving data as table '{table_name}' in {path} (mode={mode})")
        
        # Configure base writer
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("path", path) \
            .option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            if mode == "overwrite":
                # Only replace data in the partitions being written
                writer = writer.option("replaceWhere", self._get_partition_filter(df, partition_cols))
        
        try:
            # Save data and create/update table in catalog
            writer.saveAsTable(table_name)
            
            # Validate if there is data and show information
            logging.info(f"Table '{table_name}' updated successfully")
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
            logging.info(f"Table '{table_name}' contains {count} records")
        except Exception as e:
            logging.error(f"Erro ao salvar tabela '{table_name}': {e}")
            raise

    def _get_partition_filter(self, df, partition_cols):
        """
        Creates the WHERE condition to replace only specific partitions.
        
        Args:
            df (DataFrame): DataFrame with the data to be written
            partition_cols (list): List of partitioning columns
            
        Returns:
            str: WHERE condition to filter partitions.
                 Example: "(DT_REFE = '2016-01-01')"
        """
        
        conditions = []
        
        for col in partition_cols:
            distinct_values = [row[0] for row in df.select(col).distinct().collect()]
            
            formatted_values = []
            for value in distinct_values:
                if isinstance(value, (date, str)):
                    formatted_values.append(f"{col} = '{value}'")
                elif value is None:
                    continue
                else:
                    formatted_values.append(f"{col} = {value}")
            
            if formatted_values:
                conditions.append(f"({' OR '.join(formatted_values)})")
        
        if not conditions:
            return "1=1" 
        
        return " AND ".join(conditions)

    def write_parquet(self, df, path, mode="overwrite"):
        """
        Writes data to a Parquet file.

        Args:
            df (DataFrame): Spark DataFrame to be written.
            path (str): Path to the Parquet file.
            mode (str): Write mode (default is "overwrite").
        """
        logging.info(f"Writing data to Parquet file: {path} (mode={mode})")
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        df.write \
            .mode(mode) \
            .parquet(path)
            
        logging.info(f"Parquet file written successfully to {path}")
