import pandas as pd
import sys
import os
import ckanapi
import requests
from config import settings

APIKEY = settings.CKANApiKey
owner_org = settings.CKANOrg

ckan = ckanapi.RemoteCKAN(settings.CKANUrl, apikey=APIKEY)

def upload_dataset(name):
    pkg_name = name.replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    pkg_title =name
    
    # Create Package
    try:
        package = ckan.action.package_create(name=pkg_name,
                                         title=pkg_title, owner_org=owner_org)

        return package['id']
        
    except ckanapi.ValidationError as e:
        if (e.error_dict['__type'] == 'Validation Error' and
           e.error_dict['name'] == ['That URL is already in use.']):
            print (f'"{pkg_title}" package already exists')
            return
        else:
            raise

    

def upload_resource(res_dict):

    package_id    = res_dict['package_id']
    resource_name = res_dict['resource_name']
    data          = res_dict['data']


    data.to_csv('data110.csv')
    file_path  = 'data110.csv'


    # Create Resource
    try:
        package = ckan.action.resource_create(package_id=package_id, name=resource_name, 
                                                upload=open(file_path, 'rb'))
    except ckanapi.ValidationError as e:
        print(e)                                             


def main():

    pkg_dict = {'pkg_name': 'test2', 'pkg_title': 'test2', 'owner_org': 'test'}

    upload_dataset(pkg_dict)    

    data = [['tom', 10], ['nick', 15], ['juli', 14]]
 
    # Create the pandas DataFrame
    df = pd.DataFrame(data, columns = ['Name', 'Age'])
 

    res_dict = {'package_id': 'test2', 'resource_name': 'test10', 'data': df}

    upload_resource(res_dict)

if __name__ == '__main__':
    main()
    




