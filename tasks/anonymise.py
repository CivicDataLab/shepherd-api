import re

import pandas as pd


def anonymize(data: pd.DataFrame):
    to_replace = input("Enter the thing u want to change\n")
    val = input("Enter replace String\n")
    col = input("Which column u need to change?\n")

    df_updated = data[col].str.replace(re.compile(to_replace, re.IGNORECASE), val)
    df_updated = df_updated.to_frame()
    data[col] = df_updated[col]
    data.to_csv('anonymised.csv', index=False)


data = pd.read_csv('E:\git\my_try\shepherd-api\data110.csv')
anonymize(data)
