from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, lower, concat, substring, lit

class ColumnTransformation(ABC):
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        pass

class DateTransformation(ColumnTransformation):
    def __init__(self, input_col: str, output_col: str, date_format: str):
        self.input_col = input_col
        self.output_col = output_col
        self.date_format = date_format

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            self.output_col,
            concat(
                substring(col(self.input_col), 7, 4),  # ano
                lit("-"),
                substring(col(self.input_col), 1, 2),  # mês
                lit("-"),
                substring(col(self.input_col), 4, 2)   # dia
            )
        )
    
class LowerCaseTransformation(ColumnTransformation):
    def __init__(self, column: str):
        self.column = column

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.column, lower(col(self.column)))

class DataValidator:
    def validate_required_columns(self, df: DataFrame, required_columns: list) -> bool:
        missing_columns = [col for col in required_columns if col not in df.columns]
        return len(missing_columns) == 0

    def validate_non_null_columns(self, df: DataFrame, columns: list) -> bool:
        return df.dropna(subset=columns).count() == df.count()
