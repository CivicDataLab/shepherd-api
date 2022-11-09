import random
import re

import pandas as pd

data = pd.read_csv('weather.csv')

col = 'city'
# print(data)
option = input("enter the option - 1. replace all char(all)\n 2.replace nth char(nth)\n 3.Retain first n chars(fn)")
n = input("enter n")
if option == "nth":
    n = int(n)-1
special_char = input("Choose special char. 1. *\n 2.$\n, 3.random")
# replace_val = ""
df_col_values = data[col].values.tolist()

new_vals = []
for val in df_col_values:
    if option == "all":
        if special_char == "random":
            replace_val = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=len(val)))
            new_vals.append(replace_val)
        else:
            replace_val = special_char * len(val)
            new_vals.append(replace_val)
    elif option == "nth":
        if special_char == "random":
            replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=1))
            replace_val = val[0:int(n)] + replacement + val[int(n)+1:]
            new_vals.append(replace_val)
        else:
            replace_val = val[0:int(n)] + special_char + val[int(n)+1:]
            new_vals.append(replace_val)
    elif option == "fn":
        if special_char == "random":
            replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=(len(val) - int(n))))
            replace_val = val[:int(n)] + replacement
            new_vals.append(replace_val)
        else:
            replace_val = val[:int(n)] + (special_char * (len(val) - int(n)))
            print("inside fn and rep is", replace_val)
            new_vals.append(replace_val)
data[col] = new_vals
print(data)
# df_updated = data[col].replace(data.loc[data[col]],"*") #.replace(data[col][:], replace_char)
# data[col] = df_updated[col]

print(df_col_values)
