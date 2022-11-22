# Create a simple dataframe

# importing pandas as pd
import pandas as pd
import numpy as np

# creating a dataframe
df = pd.DataFrame({'A': ['John', 'Boby', 'Mina', 'Peter', 'Nicky', "XYZ", 'Peter'],
	'B': ['Masters', 'Graduate', 'Graduate', 'Masters', 'Graduate', 'Graduate', 'Masters'],
	'C': [27, 23, 21, 23, 24, 21, 21],
    'D':["q","q","q","q","q", "q", "q"]})
print(df)
# Creates a pivot table dataframe
table = pd.pivot_table(df, index ='C',
						columns =['B'],aggfunc = "count")

print(table)
