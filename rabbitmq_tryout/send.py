import pandas as pd
import numpy as np

df = pd.read_csv("weather.csv")
schema = [{"key": "date", "format": "String", "description": "date field"},
          {"key": "city", "format": "String", "description": "city field"},
          {"key": "temperature", "format": "String", "description": "temp. field"},
          {"key": "humidity", "format": "String", "description": "humidity field"}]
index = "city"
columns = "date"
values = ""
k = df.pivot(index=index, columns=columns)
# for sc in schema:
#         if sc['key'] == index:
#                 sc['key'] = ""
#                 sc['format'] = ""
#                 sc['description'] = ""
#         if values == "":
#                 for col in k.columns:
print(k.columns)
for col in k.columns:
    print(col)
if values == "":
    schema = []
    for col in k.columns:
        keys = ""
        for i in col:
            keys = keys + i + " "
        temp_sc = {"key": keys}
        schema.append(temp_sc)
print(schema)
