import pandas as pd
import numpy as np

# creating a dataframe
from pandas.io.json import build_table_schema

df = pd.DataFrame({'A': ['John', 'Boby', 'Mina', 'Peter', 'Nicky', "XYZ", 'Peter'],
	'B': ['Masters', 'Graduate', 'Graduate', 'Masters', 'Graduate', 'Graduate', 'Masters'],
	'C': [27, 23, 21, 23, 24, 21, 21],
    'D':["q","q","q","q","q", "q", "q"]})
# print(df)
# Creates a pivot table dataframe
table = pd.pivot_table(df, index ='C',
						columns =['B'],aggfunc = "count")
print(table)
inferred_schema = build_table_schema(table)
fields = inferred_schema['fields']
# print(fields)
new_schema = []
for field in fields:
	key = field['name']
	description = ""
	format = "integer"
	if isinstance(key, tuple):
		key = "-".join(map(str, key))
		new_schema.append({"key": key, "format": format, "description": description})
print(new_schema)
# new_schema = []
# for field in fields:
#     key = field['name']
#     description = ""
#     format = field['type']