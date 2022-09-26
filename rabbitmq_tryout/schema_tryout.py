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
data_schema = k.convert_dtypes(infer_objects=True, convert_string=True,
                                               convert_integer=True, convert_boolean=True, convert_floating=True)
names_types_dict = data_schema.dtypes.astype(str).to_dict()
# print(names_types_dict)



# for col in k.columns:
#     print(col)
if values == "":
    schema = []
    for col in k.columns:
        format = names_types_dict[col]
        keys = ""
        for i in col:
            keys = keys + i + " "
        keys.strip()
        temp_sc = {"key": keys, "format": format}
        schema.append(temp_sc)
print(schema)
