import csv
import json
import bisect
import os
import shutil
from functools import reduce

import pandas as pd
from prefect import get_run_logger, flow
import time

import graphql_service
from graphql_service import *
from datetime import datetime, timedelta
from dateutil import relativedelta

config = ConfigParser()

config.read("config.ini")

note_string = os.environ.get('HVD_NOTE', config.get("datapipeline", 'HVD_NOTE'))
print(note_string)
params_file = open("dataset_params.json")
default_params_json = json.load(params_file)


def custom_round(num):
    if num - int(num) >= 0.5:
        return int(num) + 1
    else:
        return int(num)


def get_weight(key, params_json):
    return params_json[key]["weight"]


def send_info_to_prefect_cloud(info: str):
    prefect_logger = get_run_logger()
    prefect_logger.info(info)


def get_num_of_months(published_date):
    try:
        date_format = "%Y-%m-%d"
        published_date_obj = datetime.strptime(published_date, date_format).date()
        today = datetime.today()
        rd = relativedelta.relativedelta(today, published_date_obj)
        num_of_months = rd.years * 12 + rd.months
        return num_of_months
    except:
        return 0


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
    if distributions_count == 0:
        csv_percentage = 0
        json_percentage = 0
        xml_percentage = 0
        pdf_percentage = 0
    else:
        csv_percentage = custom_round((csv_count / distributions_count) * 100)
        json_percentage = custom_round((json_count / distributions_count) * 100)
        xml_percentage = custom_round((xml_count / distributions_count) * 100)
        pdf_percentage = custom_round((pdf_count / distributions_count) * 100)
    print("csv percentage----", csv_percentage)
    print("json prctng--", json_percentage)
    print("xml prcentg--", xml_percentage)
    print("pdf percentage--", pdf_percentage)
    csv_rating = calculate_rating_for_numerical_params("formats.params.csv", csv_percentage, params_json)
    json_rating = calculate_rating_for_numerical_params("formats.params.json", json_percentage, params_json)
    xml_rating = calculate_rating_for_numerical_params("formats.params.xml", xml_percentage, params_json)
    pdf_rating = calculate_rating_for_numerical_params("formats.params.pdf", pdf_percentage, params_json)
    print("csv-rating", csv_rating)
    print("json-rating", json_rating)
    print("xml rating", xml_rating)
    print("pdf rating", pdf_rating)
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
    # idx = 0
    for dataset in all_datasets:
        # idx += 1
        # if dataset['_id'] == "233" or dataset['_id'] == '235' or dataset['_id'] == '236':
        #     continue
        dataset_ids.append(dataset["_id"])
        # if idx == 4:
        #     break
    rating_details_df = pd.DataFrame(
        columns=["Dataset Name", "Technical Interoperability Rating", "Technical Interoperability Weight",
                 "Timeliness Rating", "Timeliness Weight", "User Rating - Rating",
                 "User Rating - Weight",  "User Interaction Rating","User Interaction Weight",
                 "Download-rating", "Download-rating Weight", "Downloads per month rating",
                 "Downloads per month Weight", "Total Rating"])

    for dataset_id in dataset_ids:

        rating_list = []  # holds all the ratings
        weights_list = []  # holds all the weights
        response = graphql_service.get_dataset(dataset_id)
        print(response)
        dataset_name = response['data']['dataset']['title']
        # get percentage of csvs, jsons, xmls and pdfs
        tech_interoperability_rating = get_technical_interoperability_rating(response, params_json)
        rating_list.append(tech_interoperability_rating)
        weights_list.append(get_weight('formats', params_json))  # Max tech. interoperability score

        update_frequency = response['data']['dataset']['update_frequency']
        update_frequency = str(update_frequency).replace(" ", "").lower()
        update_frequency_rating = calculate_rating_for_string_params("update_frequency", update_frequency, params_json)
        rating_list.append(update_frequency_rating)
        weights_list.append(get_weight('update_frequency', params_json))

        average_rating = response['data']['dataset']['average_rating']
        average_rating_contribution = calculate_rating_for_numerical_params("average_rating",
                                                                            round(custom_round(average_rating), 1),
                                                                            params_json)
        rating_list.append(average_rating_contribution)
        weights_list.append(get_weight('average_rating', params_json))

        user_interaction = len(response['data']['dataset']['datasetratings_set'])
        user_interaction_rating = calculate_rating_for_numerical_params("user_interaction", user_interaction,
                                                                        params_json)
        rating_list.append(user_interaction_rating)
        weights_list.append(get_weight('user_interaction', params_json))

        downloads = response['data']['dataset']['download_count']
        download_rating = round(calculate_rating_for_numerical_params("downloads", int(downloads), params_json), 2)
        rating_list.append(download_rating)
        weights_list.append(get_weight('downloads', params_json))

        published_date = response['data']['dataset']['published_date']
        if published_date is not None:
            try:
                published_date = published_date[:9]
                num_of_months = get_num_of_months(published_date)
                if num_of_months == 0:
                    downloads_per_month = downloads
                else:
                    downloads_per_month = downloads // num_of_months
                downloads_per_month_rating = calculate_rating_for_numerical_params("downloads_per_month",
                                                                                   downloads_per_month, params_json)
                rating_list.append(downloads_per_month_rating)
                weights_list.append(get_weight('downloads_per_month', params_json))
            except:
                rating_list.append(0)
                weights_list.append(get_weight('downloads_per_month', params_json))
        else:
            rating_list.append(0)
            weights_list.append(get_weight('downloads_per_month', params_json))

        print(rating_list)
        print(round(sum(rating_list), 2), "????")

        log_string = f'''
        The dataset - {dataset_name} has got a total rating of {round(sum(rating_list), 1)} with the following contribution
        Technical Interoperability - {round(rating_list[0], 1)}
        Timeliness - {round(rating_list[1], 1)}
        User Rating - {round(rating_list[2], 1)}
        User Interaction - {round(rating_list[3], 1)}
        Downloads - {round(rating_list[4], 1)}
        Downloads per month - {round(rating_list[5], 1)}
        '''
        print("writing to file...")
        print(len(rating_details_df.columns), "[[[[[")
        print(weights_list, ">>>>>>")

        rating_details_df.loc[len(rating_details_df)] = [dataset_name,  # Dataset Name
                                                         round(rating_list[0], 1),  # Technical Interoperability Rating
                                                         weights_list[0],  # Technical Interoperability Weight
                                                         round(rating_list[1], 1),  # Timeliness Rating
                                                         weights_list[1],  # Timeliness Weight
                                                         round(rating_list[2], 1),  # User Rating - Rating
                                                         weights_list[2],  # User Rating - Weight
                                                         round(rating_list[3], 1),  # User Interaction Rating
                                                         weights_list[3],  # User Interaction Weight
                                                         round(rating_list[4], 1),  # Download-rating
                                                         weights_list[4],  # Download-rating Weight
                                                         round(rating_list[5], 1),  # Downloads per month rating
                                                         weights_list[5],  # Downloads per month Weight
                                                         round(sum(rating_list), 1)]  # Total rating

        send_info_to_prefect_cloud(log_string)
        patch_dataset(dataset_id, round(sum(rating_list), 1))
    ratings_file_name = "NIC_IDP_HVDClassifier" + (time.strftime("%Y-%m-%d-%H.%M.%S")) + ".csv"
    rating_details_df.to_csv(ratings_file_name, index=False)
    with open(ratings_file_name, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Note:', note_string])
    try:
        email_url = os.environ.get('EMAIL_URL', config.get("datapipeline", "EMAIL_URL"))
        headers = {}
        files = {'file': open(ratings_file_name, 'rb')}
        response = requests.request("POST", email_url, headers=headers, files=files)
        response_json = response.text
        print('------mail response', response_json)
    except Exception as e:
        raise
        print(str(e))
    # destination_dir = os.getcwd()
    # print(destination_dir)
    # destination_file = destination_dir + "/" + ratings_file_name
    # print(destination_file)
    # if os.path.isfile(ratings_file_name):
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
