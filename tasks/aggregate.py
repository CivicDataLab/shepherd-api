import pandas as pd

def aggregate(data, index,columns, values):
    print(data)
    print(type(data))
    aggregated = pd.pivot(data, index=index, columns=columns, values=values)
    print(aggregated)
data = pd.read_csv('E:\git\my_try\shepherd-api\data110.csv')
index = input("enter index")
columns = input("enter columns that u need")
values = input("enter values")
columns = columns.split(",")
values = values.split(",")

aggregate(data, index, columns, values)