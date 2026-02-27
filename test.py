from eda.error_detection import ErrorDetection
from eda.constants import ColumnDataType
import pandas as pd

df = pd.read_csv("test_file.csv")

# Configure ErrorDetection to analyze the 'Data' column as categorical
params = {"columns": {"Data": ColumnDataType.CATEGORICAL.value}}
ed = ErrorDetection(params, df)

analysis = ed.analyze()
print(analysis)
converted = ed.act(analysis)
print(converted)