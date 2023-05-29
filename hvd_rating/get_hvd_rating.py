import pandas as pd

import graphql_service
from graphql_service import *

def set_hvdr_to_zero(dataset_id):
    for d_id in dataset_id:
        print(patch_dataset(d_id, 0.001))


all_datasets = get_all_datasets()
dataset_ids = []
print(len(all_datasets))
for dataset in all_datasets:
    dataset_ids.append(dataset["_id"])
# set_hvdr_to_zero(dataset_ids)
name_list = []
hvd_list = []
id_list = []
count = 0
for d_id in dataset_ids:
    count += 1
    dataset_detail = get_dataset(d_id)
    print(dataset_detail)
    id_list.append(d_id)
    name = dataset_detail["data"]["dataset"]["title"]
    name_list.append(name)
    hvd_rating = dataset_detail["data"]["dataset"]["hvd_rating"]
    hvd_list.append(hvd_rating)
df = pd.DataFrame({
    "Dataset ID": id_list,
    "Dataset Name": name_list,
    "HVD-rating": hvd_list
})
df.to_csv("hvd_rating/hvd-ratings.csv", index = False)