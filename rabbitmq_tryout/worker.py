import pandas as pd

df = pd.read_csv("bos2021ModC.csv")
print(df.drop("level", axis=1))
