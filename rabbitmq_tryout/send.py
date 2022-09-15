import time

import pandas as pd
import requests
from io import StringIO

# def read_data(data_url):
#     all_data = pd.read_csv(data_url)
#     all_data.fillna(value="", inplace=True)
#     return all_data
#
# start = time.time()
# response = requests.get("http://idpbe.civicdatalab.in/download/141")
# data = response.text
# data_frame = pd.read_csv(StringIO(data), sep=",")
# print(data_frame)
#
# # data = read_data("http://idpbe.civicdatalab.in/download/141")
#
# print("***************************")
# data_new = data.convert_dtypes(infer_objects=True, convert_string=True, convert_integer=True, convert_boolean=True, convert_floating=True)
# data_new.info()
# print("time to read data...",time.time() - start)
# names_types_dict = data_new.dtypes.astype(str).to_dict()
# print(names_types_dict)
#







