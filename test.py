from eda.error_detection import ErrorDetection
from eda.constants import ColumnDataType
import pandas as pd

from eda.missing_value_evaluation import MissingValueEvaluation

df = pd.read_csv("test_file.csv")

# Configure ErrorDetection to analyze the 'Data' column as categorical
params = {"columns": {"Categorical": ColumnDataType.CATEGORICAL.value, 
                      "Distance": ColumnDataType.NUMERIC.value, 
                      "Weight": ColumnDataType.NUMERIC.value},
          "analysis_sample_size": 30,
          "category_threshold_percentage": 0.05}
ed = ErrorDetection(params, df)

analysis = ed.analyze()
print(analysis)
converted = ed.act(analysis)
print(converted)
print(converted.shape)

params = {"columns": {"Categorical": ColumnDataType.CATEGORICAL.value, 
                      "Distance": ColumnDataType.NUMERIC.value, 
                      "Weight": ColumnDataType.NUMERIC.value},
                      "columns_drop_threshold_percentage": 0.6,
                      "rows_drop_threshold_percentage": 0.6}
mve = MissingValueEvaluation(params, converted)
analysis = mve.analyze()
print(analysis)
cleaned = mve.act(analysis)
print(cleaned)
print(cleaned.shape)