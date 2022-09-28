import pandas as pd
import numpy as np
from pandas.io.json import build_table_schema

df = pd.read_csv("books.csv")
index = "title"
columns = ["price", "author"]
values = []
k = df.pivot(index=index, columns=columns, values=values)
print(k)