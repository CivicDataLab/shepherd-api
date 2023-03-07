import json
import os

import requests
from access_token_decorator import get_sys_token
from configparser import ConfigParser
import os

config = ConfigParser()

config.read("config.ini")

graph_ql_url = os.environ.get('GRAPH_QL_URL', config.get("datapipeline", "GRAPH_QL_URL"))


@get_sys_token
def resource_query(res_id, access_token=None):
    query = f"""
    {{
      resource(resource_id: {res_id}) {{
        id
        title
        description
        issued
        modified
        status
        masked_fields
        dataset {{
          id
          title
          description
          issued
          remote_issued
          remote_modified
          period_from
          period_to
          catalog{{
            organization{{
             id
            }}
          }}
          update_frequency
          modified
          status
          funnel
          action
          dataset_type
        }}
        schema {{
          id
          key
          format
          description
          path
          parent {{
          key
          }}
          parent_path
          array_field {{
          key
          }}
        }}
        file_details {{
          format
          file
          remote_url
        }}
      }}
    }}
    """
    headers = {"Authorization": access_token}  # {"Authorization": "Bearer YOUR API KEY"}
    try:
        request = requests.post(graph_ql_url, json={'query': query}, headers=headers)
    except Exception as e:
        print(str(e))
    return json.loads(request.text)


@get_sys_token
def create_resource(resource_name, description, schema, file_format, files, org_id, dataset_id, access_token=None):
    print(file_format, "88888888")
    print(type(files))
    query = f"""mutation 
        mutation_create_resource($file: Upload!) 
        {{create_resource(
                    resource_data: {{ title:"{resource_name}", description:"{description}",    
                    dataset: "{dataset_id}", status : "",
                    schema: {schema}, file_details:{{format: "{file_format}", file: $file,  remote_url: ""}}
                    }})
                    {{
                    resource {{ id }}
                    }}
                    }}
                    """
    print(query)
    variables = {"file": None}
    map = json.dumps({"0": ["variables.file"]})
    headers = {"Authorization": access_token, "organization": org_id}
    operations = json.dumps({
        "query": query,
        "variables": variables,
        "operationName": "mutation_create_resource",
    })
    try:
        response = requests.post(graph_ql_url, data={"operations": operations,
                                                     "map": map}, files=files, headers=headers)
        response_json = json.loads(response.text)
        print(response_json)
        return response_json
    except Exception as e:
        print(str(e))
        return None


@get_sys_token
def update_resource(res_details, file_format, schema, files, org_id, access_token=None):
    variables = {"file": None}
    map = json.dumps({"0": ["variables.file"]})
    query = f"""
                    mutation($file: Upload!) {{update_resource(resource_data: {{
                    id:{res_details['data']['resource']['id']}, 
                    title:"{res_details['data']['resource']['title']}", 
                    description:"{res_details['data']['resource']['description']}",   
                    dataset:"{res_details['data']['resource']['dataset']['id']}",
                    status:"{res_details['data']['resource']['status']}",  
                    file_details:{{ format:"{file_format}", file:$file, 
                    remote_url:"{res_details['data']['resource']['file_details']['remote_url']}" }},
                    schema:{schema},
                    }})
                    {{
                    success
                    errors
                    resource {{ id }}
                }}
                }}"""

    print(query)
    headers = {"Authorization": access_token, "organization": org_id}
    operations = json.dumps({
        "query": query,
        "variables": variables
    })
    try:
        response = requests.post(graph_ql_url, data={"operations": operations, "map": map},
                                 files=files, headers=headers)
        print(response)
        response_json = json.loads(response.text)
        return response_json
    except:
        return None


@get_sys_token
def get_dataset(dataset_id, access_token=None):
    query = f"""
    {{
    dataset(dataset_id: {dataset_id}) {{
    id
    title
    description
    language
    period_from
    period_to
    update_frequency
    spatial_coverage
    spatial_resolution
    hvd_rating
    resource_set{{
    id
    title
    description
    filedetails{{
    format
    }}
    apidetails{{
    default_format
    }}
    }}
    tags{{
    id
    name
    }}
    additionalinfo_set {{
      id
      title
      description
      issued
    }}
    average_rating
  }}
    }}
    """
    headers = {"Authorization": access_token}  # {"Authorization": "Bearer YOUR API KEY"}
    try:
        request = requests.post(graph_ql_url, json={'query': query}, headers=headers)
        return json.loads(request.text)
    except Exception as e:
        print(str(e))
        return None


def get_all_datasets(access_token=None):
    try:
        url = "https://dev.backend.idp.civicdatalab.in/facets/?size=1000"
        request = requests.get(url)
        response = request.json()
        # print(response)
    except Exception as e:
        print(str(e))

    return response["hits"]["hits"]

@get_sys_token
def patch_dataset(dataset_id, hvd_rating, access_token = None):
    query = f"""
        mutation{{
            patch_dataset(dataset_data: {{
                id: {dataset_id}
                hvd_rating:{hvd_rating}
            }}
            )
        {{
            success
            errors
            dataset{{
                id
                }}
            }}
        }}
    """
    print(query)
    headers = {"Authorization": access_token}
    try:
        response = requests.post(graph_ql_url, json={'query': query}, headers=headers)
        print(response)
        response_json = json.loads(response.text)
        print(response_json)
        return response_json
    except Exception as e:
        print(e)
        return None