from .eda_module import EdaModule
from typing import override
from .constants import ImputationStrategy
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
        Drop columns with missing percentage above threshold.
        Otherwise fill missing values using mean or median.
        Then drop rows exceeding row threshold.
        """

        columns_to_drop = []

        for column, result in analysis.items():
            missing_pct = result["missing_percentage"]

            if missing_pct > self._columns_drop_threshold_percentage:
                columns_to_drop.append(column)
            else:
                if pd.api.types.is_numeric_dtype(self._data[column]):
                    if self._fill_strategy == ImputationStrategy.MEAN:
                        fill_value = self._data[column].mean()
                    elif self._fill_strategy == ImputationStrategy.MEDIAN:
                        fill_value = self._data[column].median()
                    else:
                        raise ValueError(f"Unsupported fill strategy: {self._fill_strategy}")

                    self._data[column] = self._data[column].fillna(fill_value)

        self._data.drop(columns=columns_to_drop, inplace=True)

        row_missing_percentages = self._data.isna().mean(axis=1)
        rows_to_drop = row_missing_percentages[
            row_missing_percentages > self._rows_drop_threshold_percentage
        ].index

        self._data.drop(index=rows_to_drop, inplace=True)

        return self._data