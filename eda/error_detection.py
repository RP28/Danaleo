from .eda_module import EdaModule
from .constants import ColumnDataType, DebuggerErrorMessages
from typing import override
import pandas as pd
from pandas.api import types as pd_types
import random
from utils.json_utils import load_danaleo_json
from utils.semantic_parser import parse_numeric_column

class ErrorDetection(EdaModule):

    def __init__(self, params, data):
        super().__init__(params, data)
        self._rows_drop_threshold_percentage = params.get("rows_drop_threshold", 0.3)
        self._columns_drop_threshold_percentage = params.get("columns_drop_threshold", 0.3)
        self._danaleo_config = load_danaleo_json()
        self._value_convertor = None

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
                    "sample_non_convertible_values": [],
                }
            }
        
        converted = pd.to_numeric(series, errors="coerce")
        non_convertibles = series[converted.isna() & series.notna()]
        sample_non_convertible_values = random.sample(non_convertibles.tolist(), 
                                                      min(self._danaleo_config["eda"]["sample_size"], len(non_convertibles)))
        return {
            column: {
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
    def act(self, analysis, **kwargs):
        self._value_convertor = kwargs.get("value_convertor", None)
        for column, data_type in self._params.get("columns", {}).items():
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    return self._act_numeric(column, analysis[column])
                case ColumnDataType.STRING.value:
                    return self._act_string(column, analysis[column])
                case ColumnDataType.DATE.value:
                    return self._act_date(column, analysis[column])
                case ColumnDataType.BOOLEAN.value:
                    return self._act_boolean(column, analysis[column])
                case _:
                    raise ValueError(DebuggerErrorMessages.UNSUPPORTED_DATA_TYPE.value)
                
    def _act_numeric(self, column, analysis):
        """Apply a numeric fix/conversion after analysis has run.

        * If an external `value_convertor` was supplied via kwargs it will be
          invoked (allows custom logic).  Otherwise we fall back to the
          bundled semantic parser which now works vectorized.
        """
        series = self._data[column]
        sample = analysis.get("sample_non_convertible_values", [])

        if self._value_convertor is not None:
            return self._value_convertor(series, sample)

        # default converter uses semantic parser's vectorized routine
        return parse_numeric_column(series, sample)

    def _act_string(self, column, analysis):
        pass

    def _act_date(self, column, analysis):
        pass

    def _act_boolean(self, column, analysis):
        pass
    