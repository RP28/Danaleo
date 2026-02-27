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

class UserErrorMessages(Enum):
    DATA_NOT_PD_DATAFRAME = "ERR01: Data not a pandas DataFrame or Series"
    PARAMS_NOT_DICT = "ERR02: Params not a dictionary"
    PARAMS_DATA_COLUMN_MISMATCH = "ERR03: Column '{column}' specified in params not found in data"

class DebuggerErrorMessages(Enum):
    UNSUPPORTED_DATA_TYPE = "ERR10: Unsupported data type for analysis"

__all__ = [
    "ColumnDataType",
    "DataAction",
    "UserErrorMessages",
    "DebuggerErrorMessages"
]
