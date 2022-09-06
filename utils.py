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
    resource_name = res_dict['resource_name']
    data = res_dict['data']
    description = "Executing " + resource_name + " on user provided data"
    data.to_csv('data110.csv')
    file_path = 'data110.csv'
    # working ....
    # payload = {
    #     'operations': '{"query":"mutation mutation_create_resource($file: Upload!) {\\n  create_resource(\\n    '
    #                   'resource_data: {file: $file, title: \\"%s\\", description: \\"%s\\", '
    #                   'dataset: \\"5\\", remote_url: \\"\\", format: \\"CSV\\"}\\n  ) { resource { id }  '
    #                   '}\\n}\\n","variables":{"file":null},"operationName":"mutation_create_resource"}' % (
    #                   resource_name, description),
    #     'map': '{"0":["variables.file"]}'}    



    files = [
        ('0', ('data110.csv', open(file_path, 'rb'), 'text/csv'))
    ]
    url = "http://idpbe.civicdatalab.in/graphql"
    return random.randint(1, 1000000)
    # response = requests.request("POST", url, data=payload, files=files)
    # response_json = json.loads(response.text)
    # return (response_json['data']['create_resource']['resource']['id'])

# def main():
#
#     # pkg_dict = {'pkg_name': 'test2', 'pkg_title': 'test2', 'owner_org': 'test'}
#
#     # upload_dataset(pkg_dict)
#
#     data = [['tom', 10], ['nick', 15], ['juli', 14]]
#
#     # Create the pandas DataFrame
#     df = pd.DataFrame(data, columns = ['Name', 'Age'])
#
#
#     res_dict = {'package_id': 'test2', 'resource_name': 'test12', 'data': df}
#
#     upload_resource(res_dict)
#
# if __name__ == '__main__':
#     main()


def update_resource(res_dict):
    res_details = res_dict['res_details']
    data = res_dict['data']
    data.to_csv('data110.csv')
    
    file_path = 'data110.csv'
    file      = open(file_path, 'rb')
    variables = {"file": None}  
    map       = json.dumps({ "0": ["variables.file"] })
    
    
    query = f"""
            mutation($file: Upload!) {{update_resource(resource_data: {{
            id:{res_details['data']['resource']['id']}, title:{res_details['data']['resource']['title']}, description:{res_details['data']['resource']['description']},
            file:$file,
            dataset:{res_details['data']['resource']['dataset']['id']},
            status:{res_details['data']['resource']['status']}, format:{res_details['data']['resource']['format']}, remote_url:{res_details['data']['resource']['remote_url']},
            }})
            {{
            success
            errors
            resource  {{ id }}
        }}
        }}"""
        
    operations = json.dumps({
            "query": query,
            "variables": variables
            })



 
    headers = {} 
    response = requests.post('http://idpbe.civicdatalab.in/graphql', data = {"operations": operations,"map": map}, files = {"0" : file}, headers=headers)

    response_json = json.loads(response.text)
    print(response_json)
