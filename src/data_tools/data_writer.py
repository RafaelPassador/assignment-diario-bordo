import logging

class DataWriter:
    """
    Class responsible for writing data to various formats.

    Methods:
        - write_delta: Writes data to a Delta table.
    """
    def write_delta(self, df, path, mode="overwrite", partition_cols=None):
        """
        Writes data to a Delta table.

        Args:
            df (DataFrame): Spark DataFrame to be written.
            path (str): Path to the Delta table.
            mode (str): Write mode (default is "overwrite").
            partition_cols (list, optional): List of columns to partition by.
        """
        logging.info(f"Salvando dados em {path} (modo={mode})")
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)