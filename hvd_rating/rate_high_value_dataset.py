import json
import bisect

import graphql_service
from graphql_service import *
from datetime import datetime, timedelta

params_file = open("dataset_params.json")

params_json = json.load(params_file)

rating = 0 # initial rating

def get_period(period_from, period_to):
    date_format = "%Y-%m-%d"
    from_date_object = datetime.strptime(period_from, date_format)
    to_date_object = datetime.strptime(period_to, date_format)
    difference = to_date_object - from_date_object
    difference_in_years = difference.days // 365
    print(difference_in_years)
    return difference_in_years


def find_range(lst, num):
    index = bisect.bisect_right(lst, num)
    if index == 0:
        return lst[0]
    elif index == len(lst):
        return lst[-1]
    else:
        return lst[index - 1]

def get_params_as_int(key):
    int_list = [int(num) for num in params_json[key]["params"]]
    return int_list


def calculate_rating_for_numerical_params(key, actual_count):
    """ Returns the given key's contribution to the over-all rating"""
    int_params = get_params_as_int(key)
    pos_of_actual_count = find_range(int_params, actual_count)
    sub_rating = params_json[key]["params"][str(pos_of_actual_count)]
    return params_json[key]["weight"] * sub_rating


def calculate_rating_for_string_params(key, actual_val):
    return params_json[key]["params"][actual_val] * params_json[key]["weight"]


def get_rating_and_update_dataset(datsset_id):
    rating_list = []
    response = graphql_service.get_dataset(datsset_id)
    distribution_count = len(response["data"]["dataset"]["resource_set"])
    rating_list.append(calculate_rating_for_numerical_params("distribution_count", distribution_count))
    # additional info
    additional_info = len(response["data"]["dataset"]['additionalinfo_set'])
    rating_list.append(calculate_rating_for_numerical_params("additional_info", additional_info))
    # get num of years from period and calculate rating accordingly
    period_from = response['data']['dataset']['period_from']
    period_to = response['data']['dataset']['period_to']
    if period_to is not None and period_from is not None:
        num_years = get_period(period_from, period_to)
        rating_list.append(calculate_rating_for_numerical_params("period", num_years))

    tags = len(response['data']['dataset']['tags'])
    rating_list.append(calculate_rating_for_numerical_params("tags", tags))

    language = response["data"]["dataset"]["language"]
    if language is not None:
        if str(language).lower().startswith("en"):
            language = "en"
            rating_list.append(calculate_rating_for_string_params("language", language))
        elif str(language).lower().startswith("hi"):
            language = "hi"
            rating_list.append(calculate_rating_for_string_params("language", language))

    print(patch_dataset(datsset_id, round(sum(rating_list), 1)))

    return sum(rating_list)


all_datasets = get_all_datasets()
dataset_ids = []
for dataset in all_datasets:
    dataset_ids.append(dataset["_id"])
for d_id in dataset_ids:
    rating = get_rating_and_update_dataset(d_id)
# distribution_count = 83
#

#
#
# range = find_range(dist_counts, distribution_count)
#
#
# print(rating)



