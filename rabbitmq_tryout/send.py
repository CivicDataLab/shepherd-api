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
a = False
# a = "k"
a = []
if a == []:
    print("hi")






