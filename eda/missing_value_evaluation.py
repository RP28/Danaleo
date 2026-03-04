from .eda_module import EdaModule
from typing import override
from .constants import ColumnDataType, DataAction
import pandas as pd

class MissingValueEvaluation(EdaModule):

    def __init__(self, params, data):
        super().__init__(params, data)
        self._columns_drop_threshold_percentage = self._params.get("columns_drop_threshold_percentage", 0.6)
        self._rows_drop_threshold_percentage = self._params.get("rows_drop_threshold_percentage", 0.6)

        self._numeric_outlier_threshold = self._params.get("numeric_outlier_threshold", 0.05)
        self._numeric_skew_symmetric_threshold = self._params.get("numeric_skew_symmetric_threshold", 0.5)
        self._numeric_kurtosis_normal_threshold = self._params.get("numeric_kurtosis_normal_threshold", 3.5)

        self._categorical_mode_missing_threshold = self._params.get("categorical_mode_missing_threshold", 0.15)
        self._boolean_mode_missing_threshold = self._params.get("boolean_mode_missing_threshold", 0.20)

    @override
    def analyze(self):
        """Return percentage of missing values for each column specified in params."""
        results = {}
        for column in self._params.get("columns", {}).keys():
            series = self._data[column]
            missing_percentage = series.isna().mean()
            iqr = 1.5 * (series.quantile(0.75) - series.quantile(0.25)) if self._params["columns"][column] == ColumnDataType.NUMERIC.value else None
            results[column] = {
                "missing_percentage": missing_percentage,
                "data_type": self._params["columns"][column],
                "skewness": series.skew() if self._params["columns"][column] == ColumnDataType.NUMERIC.value else None,
                "kurtosis": series.kurtosis() if self._params["columns"][column] == ColumnDataType.NUMERIC.value else None,
                "outliers_percentage": ((series < series.quantile(0.25) - iqr) | (series > series.quantile(0.75) + iqr)).mean() if self._params["columns"][column] == ColumnDataType.NUMERIC.value else None
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

        for column in self._params.get("columns", {}).keys():
            imputation_strategy = self.decide_imputation(analysis[column])
            match imputation_strategy:
                case DataAction.DROP_COLUMN.value:
                    self._data.drop(columns=[column], inplace=True)
                case DataAction.IMPUTE_MEAN_VALUE.value:
                    fill_value = self._data[column].mean()
                    self._data[column] = self._data[column].fillna(fill_value)
                case DataAction.IMPUTE_MEDIAN_VALUE.value:
                    fill_value = self._data[column].median()
                    self._data[column] = self._data[column].fillna(fill_value)
                case DataAction.IMPUTE_MODE_VALUE.value:
                    mode_value = self._data[column].mode(dropna=True)
                    if not mode_value.empty:
                        fill_value = mode_value[0]
                        self._data[column] = self._data[column].fillna(fill_value)
                case DataAction.IMPUTE_NEW_CATEGORY.value:
                    self._data[column] = self._data[column].fillna("Missing")

        return self._data
    
    def decide_imputation(self, meta):
        m = meta["missing_percentage"]
        dtype = meta["data_type"]
        skew = meta["skewness"]
        kurt = meta["kurtosis"]
        out = meta["outliers_percentage"]

        if m >= self._columns_drop_threshold_percentage:
            return DataAction.DROP_COLUMN.value

        if dtype == ColumnDataType.NUMERIC.value:

            if out >= self._numeric_outlier_threshold:
                return DataAction.IMPUTE_MEDIAN_VALUE.value

            if (
                abs(skew) <= self._numeric_skew_symmetric_threshold
                and kurt <= self._numeric_kurtosis_normal_threshold
            ):
                return DataAction.IMPUTE_MEAN_VALUE.value

            return DataAction.IMPUTE_MEDIAN_VALUE.value

        if dtype == ColumnDataType.CATEGORICAL.value:

            if m < self._categorical_mode_missing_threshold:
                return DataAction.IMPUTE_MODE_VALUE.value

            return DataAction.IMPUTE_NEW_CATEGORY.value

        if dtype == ColumnDataType.BOOLEAN.value:

            if m < self._boolean_mode_missing_threshold:
                return DataAction.IMPUTE_MODE_VALUE.value

            return DataAction.IMPUTE_NEW_CATEGORY.value

        return DataAction.IMPUTE_MEDIAN_VALUE.value
