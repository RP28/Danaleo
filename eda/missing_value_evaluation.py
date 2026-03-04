from .eda_module import EdaModule
from typing import override
from .constants import ImputationStrategy, ColumnDataType
import pandas as pd

class MissingValueEvaluation(EdaModule):

    def __init__(self, params, data):
        super().__init__(params, data)
        self._columns_drop_threshold_percentage = self._params.get("columns_drop_threshold_percentage", 0.3)
        self._rows_drop_threshold_percentage = self._params.get("rows_drop_threshold_percentage", 0.3)
        self._fill_strategy = ImputationStrategy(self._params.get("fill_strategy", "mean")) 

    @override
    def analyze(self):
        """Return percentage of missing values for each column specified in params."""
        results = {}
        for column in self._params.get("columns", {}).keys():
            series = self._data[column]
            missing_percentage = series.isna().mean()
            results[column] = {
                "missing_percentage": missing_percentage
            }
        return results

    @override
    def act(self, analysis):
        """
        Drop rows exceeding row threshold.
        Drop columns with missing percentage above threshold.
        Otherwise fill missing values.
        """

        row_missing_percentages = self._data.isna().mean(axis=1)
        rows_to_drop = row_missing_percentages[
            row_missing_percentages > self._rows_drop_threshold_percentage
        ].index

        self._data.drop(index=rows_to_drop, inplace=True)

        for column, data_type in self._params.get("columns", {}).items():
            match data_type:
                case ColumnDataType.NUMERIC.value:
                    self._act_numeric(column, analysis)
                case ColumnDataType.CATEGORICAL.value:
                    self._act_categorical(column, analysis)
                case _:
                    raise ValueError(f"Unsupported data type for missing value evaluation: {data_type}")

        return self._data
    
    def _act_numeric(self, column, analysis):
        missing_pct = analysis[column]["missing_percentage"]

        if missing_pct > self._columns_drop_threshold_percentage:
            self._data.drop(columns=[column], inplace=True)
        else:
            if self._fill_strategy == ImputationStrategy.MEAN:
                fill_value = self._data[column].mean()
            elif self._fill_strategy == ImputationStrategy.MEDIAN:
                fill_value = self._data[column].median()
            elif self._fill_strategy == ImputationStrategy.MODE:
                mode_value = self._data[column].mode(dropna=True)
                if mode_value.empty:
                    raise ValueError(f"No mode found for column {column} during imputation")
                fill_value = mode_value[0]
            else:
                raise ValueError(f"Unsupported fill strategy: {self._fill_strategy}")

            self._data[column] = self._data[column].fillna(fill_value)

    def _act_categorical(self, column, analysis):
        missing_pct = analysis[column]["missing_percentage"]

        if missing_pct > self._columns_drop_threshold_percentage:
            self._data.drop(columns=[column], inplace=True)
        else:
            mode_value = self._data[column].mode(dropna=True)
            if not mode_value.empty:
                fill_value = mode_value[0]
                self._data[column] = self._data[column].fillna(fill_value)
