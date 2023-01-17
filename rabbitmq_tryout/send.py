# import json
# import random
#
# import pandas as pd
# import requests
# #
# #
# # def skip_col(d, remove_key):
# #     if isinstance(d, dict):
# #         remove_a_key(d, remove_key)
# #     if isinstance(d, list):
# #         for each in d:
# #             if isinstance(each, dict):
# #                 remove_a_key(each, remove_key)
# #     return d
# #
# # f = open('data.json', 'rb')
# # data = json.load(f)
# # trans_data = skip_col(data, ['updated_date','title'])
# # print(trans_data)
#
# sep = "|"
# op_col_name = "result"
#
#
# def merge_cols(d, cols_list, result):
#     global final_val
#     for key in list(d.keys()):
#         if key in cols_list:
#             result.append(d[key])
#         else:
#             merge_col(d[key], cols_list, result)
#
#
# def merge_col(d, cols_list, result):
#     if isinstance(d, dict):
#         merge_cols(d, cols_list, result)
#     if isinstance(d, list):
#         for each in d:
#             if isinstance(each, dict):
#                 val_list = []
#                 for key in list(each.keys()):
#                     if key in cols_list:
#                         print("got a key")
#                         val_list.append(each[key])
#                         print(val_list, "value list...")
#                         if len(val_list) == 2:
#                             result_str = str(sep).join(val_list)
#                             each[op_col_name] = result_str
#                 merge_cols(each, cols_list, result)
#     print(result)
#
#
# f = open('data.json', 'rb')
# data = json.load(f)
# result = []
# merge_col(data, ["a", "f"], result)
# merged = str(sep).join(result)
# print(merged, "merged  ")
# if isinstance(data, dict):
#     data[op_col_name] = merged
# print(data, "data..")
import csv
import json

import pandas as pd
import pdfkit

#
# def remove_a_key(d, remove_key):
#     for key in list(d.keys()):
#         if key in remove_key:
#             del d[key]
#         else:
#             skip_col(d[key], remove_key)
#
#
# def skip_col(d, remove_key):
#     if isinstance(d, dict):
#         remove_a_key(d, remove_key)
#     if isinstance(d, list):
#         for each in d:
#             if isinstance(each, dict):
#                 remove_a_key(each, remove_key)
#     return d
#
# data = pd.read_json('data.json')
# pdata = skip_col(data, "gender")
# print(pdata)
import pandas as pd
from pandas.io.json import build_table_schema

index = ["name"]
columns = ["sub", "id"]
values = ["class"]
data = pd.read_csv("test1.csv")
agged = pd.pivot_table(data, index=index, columns=columns, values=values, aggfunc='count')
# agged.columns = agged.columns.to_flat_index()
none_list = [None]
for i in columns:
    none_list.append(i)
print(type(agged.columns))
agged = agged.rename_axis(none_list, axis=1)
agged = agged.reset_index()
print(agged, "after....")
file_path = "test.csv"
print(type(agged.columns))
agged.to_csv("test.csv", index=True)
inferred_schema = build_table_schema(agged)
fields = inferred_schema['fields']
print(fields)



