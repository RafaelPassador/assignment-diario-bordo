class DataProcessingError(Exception):
    """Base exception for data processing errors"""
    pass

class DataValidationError(DataProcessingError):
    """Exception raised when data validation fails"""
    pass

class DataTransformationError(DataProcessingError):
    """Exception raised when data transformation fails"""
    pass

class DataAggregationError(DataProcessingError):
    """Exception raised when data aggregation fails"""
    pass
