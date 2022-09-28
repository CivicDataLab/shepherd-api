import pandas as pd
import numpy as np
from pandas.io.json import build_table_schema

df = pd.read_csv("weather.csv")
schema = [{"key": "date", "format": "String", "description": "date field"},
          {"key": "city", "format": "String", "description": "city field"},
          {"key": "temperature", "format": "String", "description": "temp. field"},
          {"key": "humidity", "format": "String", "description": "humidity field"}]
index = "id"
columns = ["date", "temperature"]
values = ["city","humidity"]
k = df.pivot(index=index, columns=columns)
k.to_csv('pivoted.csv')
inferred_schema = build_table_schema(k)
fields = inferred_schema['fields']
new_schema = []
for field in fields:
    key = field['name']
    description = ""
    format = field['type']
    for sc in schema:
        print("sc[key] is ", sc["key"], " key is ", key[0])
        if sc['key'] == key or sc['key'] == key[0]:
            print("in if...")
            description = sc['description']
            print(description)
    new_schema.append({"key": key, "format":format, "description": description})
print(new_schema)

# print(k)
# new_schema = schema
# for sc in schema:
#     for col in k.columns:
#         temp_sc = {}
#         keys = ""
#         description_entry = ""
#         print("Col I got now is ", col)
#         for i in col:
#             keys = keys + i + " "
#             print("key of current schema..", sc["key"], " and value of i is ", i)
#             if sc["key"] == i:
#                  description_entry = sc["description"] + " , " + ""
#         temp_sc = {"key": keys, "description": description_entry}
#         print(temp_sc)
#         # new_schema.append(temp_sc)
# print(new_schema)
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
