from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, lower

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
        return df.withColumn(self.output_col, to_date(col(self.input_col), self.date_format))

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
