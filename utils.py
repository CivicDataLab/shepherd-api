import json
import os
import random

import ckanapi
import pandas as pd
import requests

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
    print("creating the resource..")
    res_details = res_dict['res_details']
    resource_name = res_details['data']['resource']['title']
    data = res_dict['data']
    schema = res_dict['schema']
    schema = json.dumps(schema)
    schema = schema.replace('"key"', 'key').replace('"format"', 'format').replace('"description"', 'description')
    print("schema in create resource...))))", schema)
    description = "Executing " + resource_name + " on user provided data"
    res_name_for_file = res_dict['resource_name']
    if os.path.isfile(res_name_for_file + ".json"):
        file_path = res_name_for_file + ".json"
        file_format = "json"
        os.rename(file_path, resource_name + ".json")
        files = [
            ('0', (resource_name + ".json", open(resource_name + ".json", 'rb'), 'json'))
        ]
    elif os.path.isfile(res_name_for_file + ".xml"):
        file_path = res_name_for_file + ".xml"
        file_format = "xml"
        os.rename(file_path, resource_name + ".xml")
        files = [
            ('0', (resource_name+ ".xml", open(resource_name+ ".xml", 'rb'), 'xml'))
        ]
    elif os.path.isfile(res_name_for_file + ".pdf"):
        file_path = res_name_for_file + ".pdf"
        file_format = "pdf"
        os.rename(file_path, resource_name + ".pdf")
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
    query = f"""mutation mutation_create_resource($file: Upload!) {{create_resource(
                resource_data: {{file: $file, title:"{resource_name}", description:"{description}",    
                dataset: "5", remote_url: "",  format: "{file_format}", status : "",
                schema: {schema}
                }})
                {{
                resource {{ id }}
                }}
                }}
                """
    print(query)
    variables = {"file": None}
    map = json.dumps({"0": ["variables.file"]})
    operations = json.dumps({
        "query": query,
        "variables": variables,
        "operationName": "mutation_create_resource"
    })

    response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations,
                                                                           "map": map}, files=files)
    response_json = json.loads(response.text)
    print(response_json)
    return response_json['data']['create_resource']['resource']['id']


def update_resource(res_dict):
    res_details = res_dict['res_details']
    resource_name = res_details['data']['resource']['title']
    data = res_dict['data']
    data.to_csv('data110.csv', index=False)
    schema = res_dict['schema']
    schema = json.dumps(schema)
    schema = schema.replace('"id":', 'id:').replace('"key"', 'key').replace('"format"', 'format').replace('"description"', 'description')
    res_name_for_file = res_dict['resource_name']
    if os.path.isfile(res_name_for_file + ".json"):
        file_path = res_name_for_file + ".json"
        file_format = "json"
        os.rename(file_path, resource_name + ".json")
        files = [
            ('0', (resource_name + ".json", open(resource_name + ".json", 'rb'), 'json'))
        ]
    elif os.path.isfile(res_name_for_file + ".xml"):
        file_path = res_name_for_file + ".xml"
        file_format = "xml"
        os.rename(file_path, resource_name + ".xml")
        files = [
            ('0', (resource_name + ".xml", open(resource_name + ".xml", 'rb'), 'xml'))
        ]
    elif os.path.isfile(res_name_for_file + ".pdf"):
        file_path = res_name_for_file + ".pdf"
        file_format = "pdf"
        os.rename(file_path, resource_name + ".pdf")
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

    variables = {"file": None}



    map = json.dumps({"0": ["variables.file"]})
    query = f"""
                mutation($file: Upload!) {{update_resource(resource_data: {{
                id:{res_details['data']['resource']['id']}, 
                title:"{res_details['data']['resource']['title']}", 
                description:"{res_details['data']['resource']['description']}", 
                file:   $file,  
                dataset:"{res_details['data']['resource']['dataset']['id']}",
                status:"{res_details['data']['resource']['status']}", 
                format:"{file_format}", 
                remote_url:"{res_details['data']['resource']['remote_url']}", 
                schema:{schema},
                }})
                {{
                success
                errors
                resource {{ id }}
            }}
            }}"""

    print(query)
    operations = json.dumps({
        "query": query,
        "variables": variables
    })
    headers = {}
    try:
        response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations, "map": map},
                                 files=files, headers=headers)
        print(response)
        response_json = json.loads(response.text)
        print("updateresource.", response_json)
    except Exception as e:
        print(e)

