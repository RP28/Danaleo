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
        """Run analysis for every column specified in params and merge results.

        Returns a dict mapping each column name to its individual analysis output.
        """
        results = {}
        for column, data_type in self._params.get("columns", {}).items():
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    results.update(self._analyze_numeric(column))
                case ColumnDataType.CATEGORICAL.value:
                    results.update(self._analyze_categorical(column))
                case ColumnDataType.DATE.value:
                    results.update(self._analyze_date(column))
                case ColumnDataType.BOOLEAN.value:
                    results.update(self._analyze_boolean(column))
                case _:
                    raise ValueError(DebuggerErrorMessages.UNSUPPORTED_DATA_TYPE.value)
        return results
                
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

    def _analyze_categorical(self, column):
        """Return percentage frequencies for each category in the given column.

        Result JSON format:
            { column: {"frequencies": {cat1: pct1, cat2: pct2, ...}} }
        """
        series = self._data[column]
        counts = series.value_counts(normalize=True, dropna=False)
        pct_dict = {str(idx): round(float(val), 3) for idx, val in counts.items()}
        return {column: {"frequencies": pct_dict}}

    def _analyze_date(self, column):
        pass    

    def _analyze_boolean(self, column):
        pass

    @override
    def act(self, analysis, **kwargs):
        """Apply corrective actions to all specified columns and return a new DataFrame.

        `analysis` should be the output of :meth:`analyze`.  An optional
        ``value_convertor`` may be supplied via kwargs for custom behaviour.
        """
        self._value_convertor = kwargs.get("value_convertor", None)

        for column, data_type in self._params.get("columns", {}).items():
            col_analysis = analysis.get(column, {})
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    self._data[column] = self._act_numeric(column, col_analysis)
                case ColumnDataType.CATEGORICAL.value:
                    self._data[column] = self._act_categorical(column, col_analysis)
                case ColumnDataType.DATE.value:
                    self._data[column] = self._act_date(column, col_analysis)
                case ColumnDataType.BOOLEAN.value:
                    self._data[column] = self._act_boolean(column, col_analysis)
                case _:
                    raise ValueError(DebuggerErrorMessages.UNSUPPORTED_DATA_TYPE.value)
        return self._data.drop_duplicates()
                
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

    def _act_categorical(self, column, analysis):
        """Replace categories whose observed frequency is below the configured
        `category_threshold_percentage` with missing values (`pd.NA`).
        """
        series = self._data[column]

        if self._value_convertor is not None:
            return self._value_convertor(series, analysis)

        freqs = analysis.get("frequencies", {}) or {}

        threshold = self._danaleo_config.get("eda", {}).get("category_threshold_percentage", 0.01)
        allowed = {k for k, v in freqs.items() if v >= float(threshold)}

        mask = series.isin(allowed)

        return series.where(mask, other=pd.NA)

    def _act_date(self, column, analysis):
        pass

    def _act_boolean(self, column, analysis):
        pass
    