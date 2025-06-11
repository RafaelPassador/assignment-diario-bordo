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
        
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        logging.info(f"Salvando dados como tabela '{table_name}' em {path} (modo={mode})")
        
        # Configura o writer base
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("path", path) \
            .option("mergeSchema", "true")
        
        # Se tiver partições, configura para overwrite apenas as partições
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            if mode == "overwrite":
                # Apenas substitui os dados das partições que estão sendo escritas
                writer = writer.option("replaceWhere", self._get_partition_filter(df, partition_cols))
        
        try:
            # Salva os dados e cria/atualiza a tabela no catálogo
            writer.saveAsTable(table_name)
            
            # Valida se há dados e mostra informações
            logging.info(f"Tabela '{table_name}' atualizada com sucesso")
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
            logging.info(f"Tabela '{table_name}' contém {count} registros")
        except Exception as e:
            logging.error(f"Erro ao salvar tabela '{table_name}': {e}")
            raise

    def _get_partition_filter(self, df, partition_cols):
        """
        Cria a condição WHERE para substituir apenas as partições específicas.
        
        Args:
            df (DataFrame): DataFrame com os dados a serem escritos
            partition_cols (list): Lista de colunas de particionamento
            
        Returns:
            str: Condição WHERE para filtrar as partições. 
                 Exemplo: "(DT_REFE = '2016-01-01')"
        """
        
        conditions = []
        
        for col in partition_cols:
            # Obtém valores únicos da coluna
            distinct_values = [row[0] for row in df.select(col).distinct().collect()]
            
            # Formata cada valor baseado em seu tipo
            formatted_values = []
            for value in distinct_values:
                if isinstance(value, (date, str)):
                    # Trata datas e strings com aspas simples
                    formatted_values.append(f"{col} = '{value}'")
                elif value is None:
                    # Ignora valores nulos
                    continue
                else:
                    # Para outros tipos (int, float, etc)
                    formatted_values.append(f"{col} = {value}")
            
            if formatted_values:
                # Junta os valores com OR somente se houver valores válidos
                conditions.append(f"({' OR '.join(formatted_values)})")
        
        if not conditions:
            return "1=1"  # Condição sempre verdadeira se não houver filtros
            
        # Junta as condições de cada coluna com AND
        return " AND ".join(conditions)
