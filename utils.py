import json
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
    resource_name = res_dict['resource_name']
    data = res_dict['data']
    description = "Executing " + resource_name + " on user provided data"
    data.to_csv('data110.csv')
    file_path = 'data110.csv'
    # working ....

    # 'resource_data: {file: $file, ' \
    # 'title: \\"%s\\", ' \
    # 'description: \\"%s\\", ' \
    # 'dataset: \\"5\\", ' \
    # 'remote_url: \\"\\", ' \
    # 'format: \\"CSV\\", status: \\"\\", schema:{ key: \\"\\", format:  } }'


    # payload = {
    #     'operations': '{"query":"mutation mutation_create_resource($file: Upload!) {\\n  create_resource(\\n    '
    #                   'resource_data: {file: $file, title: \\"%s\\", description: \\"%s\\", '
    #                   'dataset: \\"5\\", remote_url: \\"\\", format: \\"CSV\\", status:\\"\\", schema:{id:\\"\\", key:\\"\\", format:\\"\\", description:\\"\\"} \\n) { resource { id } }'
    #                   '\\n}\\n","variables":{"file":null},"operationName":"mutation_create_resource"}' % (
    #                   resource_name, description),
    #     'map': '{"0":["variables.file"]}'}

    payload = {
        'operations': '{"query":"mutation mutation_create_resource($file: Upload!) {\\n  create_resource(\\n    '
                      'resource_data: {file: $file, title: \\"%s\\", description: \\"%s\\", '
                      'dataset: \\"5\\", remote_url: \\"\\", format: \\"CSV\\", status:\\"\\", schema:{key:\\"\\",format:\\"\\", description:\\"\\"}}\\n  ) { resource { id }  '
                      '}\\n}\\n","variables":{"file":null},"operationName":"mutation_create_resource"}' % (
                      resource_name, description),
        'map': '{"0":["variables.file"]}'}

    files = [
        ('0', ('data110.csv', open(file_path, 'rb'), 'text/csv'))
    ]
    url = "http://idpbe.civicdatalab.in/graphql"
    # return random.randint(1, 1000000)
    response = requests.request("POST", url, data=payload, files=files)
    response_json = json.loads(response.text)
    print(response_json)
    return response_json['data']['create_resource']['resource']['id']


def update_resource(res_dict):
    print("in update..&&&&")
    res_details = res_dict['res_details']
    data = res_dict['data']
    print ('-------------------', data)
    data.to_csv('data110.csv')
    print("details..", res_details)
    file_path = 'data110.csv'
    file = open(file_path, 'rb')
    variables = {"file": None}  
    map       = json.dumps({ "0": ["variables.file"] })
    key = "key"
    format = "format"
    description = "description"
    # query = f"""
    #     mutation($file: Upload!) {{update_resource(resource_data: {{
    #     id:{res_details['data']['resource']['id']}, title:"{res_details['data']['resource']['title']}", description:"{res_details['data']['resource']['description']}", file:   $file,
    #     dataset:"{res_details['data']['resource']['dataset']['id']}",
    #     status:"{res_details['data']['resource']['status']}", format:"{res_details['data']['resource']['format']}",
    #     remote_url:"{res_details['data']['resource']['remote_url']}", schema:{{}},
    #     }})
    #     {{
    #     success
    #     errors
    #     resource {{ id }}
    # }}
    # }}"""

    query = f"""
            mutation($file: Upload!) {{update_resource(resource_data: {{
            id:{res_details['data']['resource']['id']}, title:"{res_dict['resource_name']}", description:"{res_details['data']['resource']['description']}", file:   $file,  
            dataset:"{res_details['data']['resource']['dataset']['id']}",
            status:"{res_details['data']['resource']['status']}", format:"{res_details['data']['resource']['format']}", 
            remote_url:"{res_details['data']['resource']['remote_url']}", schema:{{key: "", format:"",description:""}},
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
    response = requests.post('http://idpbe.civicdatalab.in/graphql', data = {"operations": operations,"map": map}, files = {"0" : file}, headers=headers)

    print(response)
    response_json = json.loads(response.text)
    print("updateresource.",response_json)
