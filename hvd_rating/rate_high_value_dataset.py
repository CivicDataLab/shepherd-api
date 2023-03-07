import csv
import json
import bisect
import os
import shutil
from functools import reduce

import pandas as pd
from prefect import get_run_logger, flow

import graphql_service
from graphql_service import *
from datetime import datetime, timedelta

params_file = open("dataset_params.json")
default_params_json = json.load(params_file)


def custom_round(num):
    if num - int(num) >= 0.5:
        return int(num) + 1
    else:
        return int(num)


def send_info_to_prefect_cloud(info: str):
    prefect_logger = get_run_logger()
    prefect_logger.info(info)


def get_period(period_from, period_to):
    date_format = "%Y-%m-%d"
    from_date_object = datetime.strptime(period_from, date_format)
    to_date_object = datetime.strptime(period_to, date_format)
    difference = to_date_object - from_date_object
    difference_in_years = difference.days // 365
    print(difference_in_years)
    return difference_in_years


def find_range(lst, num):
    """ Returns the position of the given value in the array of integers"""
    index = bisect.bisect_right(lst, num)
    if index == 0:
        return lst[0]
    elif index == len(lst):
        return lst[-1]
    else:
        return lst[index - 1]


def get_params_as_int(key, params_json):
    """ For the given path of key in key1.key2.key3.. format,
    traverses through the params_json file and returns the integer list of keys, that are in string
    format in the params_json
    """
    keys = key.split('.')
    try:
        result = reduce(lambda obj, key: obj[key], keys, params_json)
    except KeyError as e:
        result = None
    int_list = [int(num) for num in result["params"]]
    int_list.sort()
    return int_list, result


def calculate_rating_for_numerical_params(key, actual_count, params_json):
    """ Returns given key's contribution to the over-all rating"""
    int_params, resultant_json = get_params_as_int(key, params_json)
    # Based on the actual_count determine the score to be given to the key
    pos_of_actual_count = find_range(int_params, actual_count)
    sub_rating = resultant_json["params"][str(pos_of_actual_count)]
    # if the key is of multilevel, then we need to fetch weights of all the previous keys as well
    keys = key.split(".")
    weights = []
    # use a temp. variable params_json
    json = params_json
    for key in keys:
        try:
            # get the weight of the key and add it to the list
            weight = json[key]['weight']
            weights.append(weight)
            json = json[key]
        except:
            # if the key doesn't contain the key, then move to the subsequent dictionary
            json = json[key]
    rating = weights[0] * sub_rating
    for idx in range(1, len(weights)):
        rating *= weights[idx]
    return rating


def calculate_rating_for_string_params(key, actual_val, params_json):
    return params_json[key]["params"][actual_val] * params_json[key]["weight"]


def get_technical_interoperability_rating(dataset_response, params_json):
    distributions = dataset_response["data"]["dataset"]["resource_set"]
    distributions_count = len(distributions)
    print(distributions)
    csv_count = 0
    json_count = 0
    xml_count = 0
    pdf_count = 0
    for d in distributions:
        if d['filedetails'] is not None:
            format = str(d['filedetails']['format']).lower()
        else:
            format = str(d['apidetails']['default_format']).lower()
        if format == "csv":
            csv_count += 1
        elif format == "json":
            json_count += 1
        elif format == "pdf":
            pdf_count += 1
        elif format == "xml":
            xml_count += 1
        else:
            return 0
    csv_percentage = (csv_count // distributions_count) * 100
    json_percentage = (json_count // distributions_count) * 100
    xml_percentage = (xml_count // distributions_count) * 100
    pdf_percentage = (pdf_count // distributions_count) * 100

    csv_rating = calculate_rating_for_numerical_params("formats.params.csv", csv_percentage, params_json)
    json_rating = calculate_rating_for_numerical_params("formats.params.json", json_percentage, params_json)
    xml_rating = calculate_rating_for_numerical_params("formats.params.xml", xml_percentage, params_json)
    pdf_rating = calculate_rating_for_numerical_params("formats.params.pdf", pdf_percentage, params_json)
    tech_interoperability_rating = csv_rating + json_rating + xml_rating + pdf_rating
    return tech_interoperability_rating


@flow
def get_rating_and_update_dataset(params_json=default_params_json):
    if params_json == default_params_json:
        send_info_to_prefect_cloud("Using default params file...")
    else:
        send_info_to_prefect_cloud("Using custom params file...")
    all_datasets = get_all_datasets()
    dataset_ids = []
    # filename = "hvd_ratings.csv"
    # header = ["Dataset Name", "Distribution count rating", "Additional info rating", "Time period rating",
    #           "Tags rating", "Language Rating", "Total Rating"]
    k = 0

    for dataset in all_datasets:
        k += 1
        dataset_ids.append(dataset["_id"])
        if k==2:
            break
    rating_details_df = pd.DataFrame(columns=["Dataset Name", "Distribution count rating", "Additional info rating", "Time period rating",
               "Tags rating", "Language Rating", "Total Rating"])

    for dataset_id in dataset_ids:
        rating_list = []
        response = graphql_service.get_dataset(dataset_id)
        print(response, "%%%%%%")
        dataset_name = response['data']['dataset']['title']
        # get percentage of csvs, jsons, xmls and pdfs
        tech_interoperability_rating = get_technical_interoperability_rating(response, params_json)
        rating_list.append(tech_interoperability_rating)
        print(tech_interoperability_rating, "........")

        update_frequency = response['data']['dataset']['update_frequency']
        update_frequency_rating = calculate_rating_for_string_params("update_frequency",update_frequency, params_json)
        print(update_frequency_rating)
        rating_list.append(update_frequency_rating)

        #average_rating = response['data']['dataset']['average_rating']
        average_rating = 2.8
        average_rating_contribution = calculate_rating_for_numerical_params("average_rating", round(custom_round(average_rating),1), params_json)
        rating_list.append(average_rating_contribution)
        print(rating_list)


    #
    #
    #
    #
    #     log_string = f'''
    #     The dataset - {dataset_name} has got a total rating of {round(sum(rating_list), 1)} with the following contribution
    #     Distribution count - {round(rating_list[0], 1)}
    #     Additional info - {round(rating_list[1], 1)}
    #     Time period - {round(rating_list[2], 1)}
    #     Tags - {round(rating_list[3], 1)}
    #     Language - {round(rating_list[4], 1)}
    #     '''
    #     print("writing to file...")
    #     rating_details_df.loc[len(rating_details_df)] = [dataset_name, round(rating_list[0], 1), round(rating_list[1], 1),
    #                        round(rating_list[2], 1), round(rating_list[3], 1), round(rating_list[4], 1),
    #                        round(sum(rating_list), 1)]
    #
    #
    #     send_info_to_prefect_cloud(log_string)
    #     print(patch_dataset(dataset_id, round(sum(rating_list), 1)))
    # ratings_file_name = "Rating_details.csv"
    # destination_file = 'E:/git/my_try/shepherd-api/Rating_details.csv'
    # rating_details_df.to_csv(ratings_file_name, index=False)
    #
    # if(os.path.isfile(ratings_file_name)):
    #     shutil.copyfile(ratings_file_name, destination_file)
    #     print("File copied successfully!")


if __name__ == "__main__":
    # params_file = open("dataset_params.json")
    # # global params_json
    # params_json_from_file = json.load(params_file)

    get_rating_and_update_dataset()
# distribution_count = 83
#

#
#
# range = find_range(dist_counts, distribution_count)
#
#
# print(rating)
