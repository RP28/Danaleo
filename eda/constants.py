from enum import Enum

class ColumnDataType(Enum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    DATE = "date"
    BOOLEAN = "boolean"

class DataAction(Enum):
    DROP_ROW = "drop_row"
    DROP_COLUMN = "drop_column"
    IMPUTE_VALUE = "impute_value"

class ImputationStrategy(Enum):
    MEAN = "mean"
    MEDIAN = "median"

class UserErrorMessages(Enum):
    DATA_NOT_PD_DATAFRAME = "UERR01: Data not a pandas DataFrame or Series"
    PARAMS_NOT_DICT = "UERR02: Params not a dictionary"
    PARAMS_DATA_COLUMN_MISMATCH = "UERR03: Column '{column}' specified in params not found in data"

class DebuggerErrorMessages(Enum):
    UNSUPPORTED_DATA_TYPE = "DERR01: Unsupported data type for analysis"

__all__ = [
    "ColumnDataType",
    "DataAction",
    "UserErrorMessages",
    "DebuggerErrorMessages"
]
