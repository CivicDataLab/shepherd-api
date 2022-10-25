import json
import os
import random
from fileinput import close

import ckanapi
import pandas as pd
import requests

import graphql_service
from config import settings

APIKEY = settings.CKANApiKey
owner_org = settings.CKANOrg

ckan = ckanapi.RemoteCKAN(settings.CKANUrl, apikey=APIKEY)


def upload_dataset(name, org_name):
    pkg_name = name.replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":",
                                                                                                           "--").replace(
        "?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'",
                                                                                                                    "")[
               :100]
    pkg_title = name

    # Create Package
    try:
        # package = ckan.action.package_create(name=pkg_name,
        #                                  title=pkg_title, owner_org=org_name)

        # return package['id']
        return random.randint(1, 1000000000)
    except ckanapi.ValidationError as e:
        if (e.error_dict['__type'] == 'Validation Error' and
                e.error_dict['name'] == ['That URL is already in use.']):
            # print (f'"{pkg_title}" package already exists')
            return
        else:
            raise


def upload_resource(res_dict):
    print("in utils upload reource")
    print(res_dict)
    package_id = res_dict['package_id']
    resource_name = res_dict['resource_name']
    data = res_dict['data']

    data.to_csv('data110.csv')
    file_path = 'data110.csv'

    # Create Resource
    try:
        # resource = ckan.action.resource_create(package_id=package_id, name=resource_name,
        #                                         upload=open(file_path, 'rb'))
        # print("resource id in utils..", resource['id'])
        # return resource['id']
        return random.randint(1, 1000000000)
    except ckanapi.ValidationError as e:
        print(e)


def create_resource(res_dict):
    """ The file used to create resource can be of any format. If the pipeline has change_format
    task, the created resource can be in json/xml/pdf formats. To choose the file and to set the
    file format field in the graphql query there are some if else statements involved here.
    """
    res_details = res_dict['res_details']
    resource_name = res_details['data']['resource']['title']
    data = res_dict['data']
    schema = res_dict['schema']
    schema = json.dumps(schema)
    schema = schema.replace('"id":', 'id:').replace('"key":', 'key:').replace('"format":', 'format:').replace(
        '"description":', 'description:')
    res_name_for_file = res_dict['resource_name']
    description = "Result of the execution of pipeline named - " + res_name_for_file
    dir = "format_changed_files/"
    file_path = dir + res_name_for_file
    if os.path.isfile(file_path + ".json"):
        file_path = file_path + ".json"
        file_format = "JSON"
        os.rename(file_path, resource_name + ".json")
        file_path = resource_name + ".json"
        files = [
            ('0', (resource_name + ".json", open(resource_name + ".json", 'rb'), 'json'))
        ]
    elif os.path.isfile(file_path + ".xml"):
        file_path = file_path + ".xml"
        file_format = "XML"
        os.rename(file_path, resource_name + ".xml")
        file_path = resource_name + ".xml"
        files = [
            ('0', (resource_name + ".xml", open(resource_name + ".xml", 'rb'), 'xml'))
        ]
    elif os.path.isfile(file_path + ".pdf"):
        file_path = file_path + ".pdf"
        file_format = "PDF"
        os.rename(file_path, resource_name + ".pdf")
        file_path = resource_name + ".pdf"
        files = [
            ('0', (resource_name + ".pdf", open(resource_name + ".pdf", 'rb'), 'pdf'))
        ]
    else:
        data.to_csv(resource_name + ".csv", index=False)
        file_path = resource_name + ".csv"
        file_format = "CSV"
        files = [
            ('0', (file_path, open(file_path, 'rb'), 'text/csv'))
        ]
    # data_set = {res_details['data']['resource']['dataset']['id']}
    # query = f"""mutation
    # mutation_create_resource($file: Upload!)
    # {{create_resource(
    #             resource_data: {{ title:"{resource_name}", description:"{description}",
    #             dataset: "8", status : "",
    #             schema: {schema}, file_details:{{format: "{file_format}", file: $file,  remote_url: ""}}
    #             }})
    #             {{
    #             resource {{ id }}
    #             }}
    #             }}
    #             """
    # print(query)
    # variables = {"file": None}
    # map = json.dumps({"0": ["variables.file"]})
    # operations = json.dumps({
    #     "query": query,
    #     "variables": variables,
    #     "operationName": "mutation_create_resource"
    # })
    try:
        # response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations,
        #                                                                         "map": map}, files=files)
        response_json = graphql_service.create_resource(resource_name,description, schema, file_format, files)
        print(response_json)
        return response_json['data']['create_resource']['resource']['id']
    except Exception as e:
        print(e)
    finally:
        files[0][1][1].close()
        os.remove(file_path)


def update_resource(res_dict):
    """ Description of create_resource applies to this method as-well"""
    res_details = res_dict['res_details']
    resource_name = res_details['data']['resource']['title']
    data = res_dict['data']
    schema = res_dict['schema']
    schema = json.dumps(schema)
    schema = schema.replace('"id":', 'id:').replace('"key":', 'key:').replace('"format":', 'format:').replace(
        '"description":', 'description:')
    res_name_for_file = res_dict['resource_name']
    dir = "format_changed_files/"
    file_path = dir + res_name_for_file
    if os.path.isfile(file_path + ".json"):
        file_path = file_path + ".json"
        file_format = "JSON"
        os.rename(file_path, resource_name + ".json")
        file_path = resource_name + ".json"
        files = [
            ('0', (resource_name + ".json", open(resource_name + ".json", 'rb'), 'json'))
        ]
    elif os.path.isfile(file_path + ".xml"):
        file_path = file_path + ".xml"
        file_format = "XML"
        os.rename(file_path, resource_name + ".xml")
        file_path = resource_name + ".xml"
        files = [
            ('0', (resource_name + ".xml", open(resource_name + ".xml", 'rb'), 'xml'))
        ]
    elif os.path.isfile(file_path + ".pdf"):
        file_path = file_path + ".pdf"
        file_format = "PDF"
        os.rename(file_path, resource_name + ".pdf")
        file_path = resource_name + ".pdf"
        files = [
            ('0', (resource_name + ".pdf", open(resource_name + ".pdf", 'rb'), 'pdf'))
        ]
    else:
        data.to_csv(resource_name + ".csv", index=False)
        file_path = resource_name + ".csv"
        file_format = "CSV"
        files = [
            ('0', (file_path, open(file_path, 'rb'), 'text/csv'))
        ]

    # variables = {"file": None}
    #
    # map = json.dumps({"0": ["variables.file"]})
    # query = f"""
    #             mutation($file: Upload!) {{update_resource(resource_data: {{
    #             id:{res_details['data']['resource']['id']},
    #             title:"{res_details['data']['resource']['title']}",
    #             description:"{res_details['data']['resource']['description']}",
    #             dataset:"{res_details['data']['resource']['dataset']['id']}",
    #             status:"{res_details['data']['resource']['status']}",
    #             file_details:{{ format:"{file_format}", file:$file,
    #             remote_url:"{res_details['data']['resource']['file_details']['remote_url']}" }},
    #             schema:{schema},
    #             }})
    #             {{
    #             success
    #             errors
    #             resource {{ id }}
    #         }}
    #         }}"""
    #
    # print(query)
    # operations = json.dumps({
    #     "query": query,
    #     "variables": variables
    # })
    # headers = {}
    try:
        #     response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations, "map": map},
        #                              files=files, headers=headers)
        #     print(response)
        response_json = graphql_service.update_resource(res_details, file_format, schema, files)
        print("updateresource.", response_json)
    except Exception as e:
        print(e)
    finally:
        files[0][1][1].close()
        os.remove(file_path)
