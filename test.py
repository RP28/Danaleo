from utils import semantic_parser
import pandas as pd

df = pd.read_csv("test_file.csv")
parsed_values = semantic_parser.parse_column(df["Data"], sample_values=df["Data"])
print(parsed_values)