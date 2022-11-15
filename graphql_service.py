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
def create_resource(resource_name, description, schema, file_format, files, org_id, dataet_id, access_token=None):
    query = f"""mutation 
        mutation_create_resource($file: Upload!) 
        {{create_resource(
                    resource_data: {{ title:"{resource_name}", description:"{description}",    
                    dataset: "{dataet_id}", status : "",
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
        return response_json
    except:
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
