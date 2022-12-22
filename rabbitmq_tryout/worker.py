import numpy as np
import pandas as pd


data = pd.read_csv("sample.csv")
t_data = data.fillna({"col1":np.NAN})
print(t_data)