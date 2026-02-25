from eda_module import EdaModule
from constants import ColumnDataType, DebuggerErrorMessages
from typing import override
import pandas as pd
from pandas.api import types as pd_types
import random
from ..utils.json_utils import load_danaleo_json

class ErrorDetection(EdaModule):

    def __init__(self, params, data):
        super().__init__(params, data)
        self._rows_drop_threshold_percentage = params.get("rows_drop_threshold", 0.3)
        self._columns_drop_threshold_percentage = params.get("columns_drop_threshold", 0.3)
        self._danaleo_config = load_danaleo_json()

    @override
    def analyze(self):
        for column, data_type in self._params.get("columns", {}).items():
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    return self._analyze_numeric(column)
                case ColumnDataType.STRING.value:
                    return self._analyze_string(column)
                case ColumnDataType.DATE.value:
                    return self._analyze_date(column)
                case ColumnDataType.BOOLEAN.value:
                    return self._analyze_boolean(column)
                case _:
                    raise ValueError(DebuggerErrorMessages.UNSUPPORTED_DATA_TYPE.value)
                
    def _analyze_numeric(self, column):
        series = self._data[column]

        # Fast path: already numeric dtype
        if pd_types.is_numeric_dtype(series.dtype):
            return {
                column: {
                    "non_convertible_indices": [],
                    "sample_non_convertible_values": [],
                }
            }
        
        converted = pd.to_numeric(series, errors="coerce")
        non_convertibles = series[converted.isna() & series.notna()]
        non_convertible_indices = non_convertibles.index.tolist()
        sample_non_convertible_values = random.sample(non_convertibles.tolist(), 
                                                      min(self._danaleo_config["eda"]["sample_size"], len(non_convertibles)))
        return {
            column: {
                "non_convertible_indices": non_convertible_indices,
                "sample_non_convertible_values": sample_non_convertible_values
            }
        }

    def _analyze_string(self, column):
        pass

    def _analyze_date(self, column):
        pass    

    def _analyze_boolean(self, column):
        pass

    @override
    def policy(self, analysis, **kwargs):
        for column, data_type in self._params.get("columns", {}).items():
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    return self._policy_numeric(column, analysis[column])
                case ColumnDataType.STRING.value:
                    return self._policy_string(column, analysis[column])
                case ColumnDataType.DATE.value:
                    return self._policy_date(column, analysis[column])
                case ColumnDataType.BOOLEAN.value:
                    return self._policy_boolean(column, analysis[column])
                case _:
                    raise ValueError(DebuggerErrorMessages.UNSUPPORTED_DATA_TYPE.value)
                
    def _policy_numeric(self, column, analysis):
        pass

    def _policy_string(self, column, analysis):
        pass

    def _policy_date(self, column, analysis):
        pass

    def _policy_boolean(self, column, analysis):
        pass
    