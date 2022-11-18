import json
import os
import uuid
from io import StringIO

import pandas as pd
import requests
from background_task import background

import log_utils
from datatransform.models import Pipeline
from access_token_decorator import get_sys_token
from configparser import ConfigParser
import os
from pipeline.model_to_pipeline import task_executor

config = ConfigParser()

config.read("config.ini")

graph_ql_url = os.environ.get('GRAPH_QL_URL', config.get("datapipeline", "GRAPH_QL_URL"))

@background(queue="api_res_operation")
@get_sys_token
def api_resource_query_task(p_id, api_source_id, request_id, request_columns, request_rows, access_token=None):
    print(api_source_id)
    print(request_id)
    query = f"""{{
  resource(resource_id: {api_source_id}) {{
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
      update_frequency
      modified
      status
      funnel
      action
      dataset_type
    }}
    resourceschema_set {{
      id
      key
      format
      description
    }}
    datarequest_set {{
      id
      status
      file
      creation_date
      user
    }}
	    api_details {{
        api_source {{
          base_url
          auth_loc
          auth_type
          auth_credentials
          auth_token
          headers
        }}
      auth_required
      url_path
      response_type
    }}
  }}
}}
"""
    headers = {"Authorization": access_token}
    file_name = "api_resource-" + str(uuid.uuid4().hex)[0:5] # name of the file to be uploaded
    request = requests.post(graph_ql_url, json={'query': query}, headers=headers)
    response = json.loads(request.text)
    print(response)
    base_url = response['data']['resource']['api_details']['api_source']['base_url']
    url_path = response['data']['resource']['api_details']['url_path']
    # # headers = response['data']['api_source']['headers']
    auth_loc = response['data']['resource']['api_details']['api_source']['auth_loc'] #- header/param?
    auth_type = response['data']['resource']['api_details']['api_source']['auth_type']  #if token/uname-pwd
    param = {}
    header = {}
    if auth_loc == "HEADER":
        if auth_type == "TOKEN":
            auth_token = response['data']['resource']['api_details']['api_source']['auth_token']
            header = {"access_token":auth_token}
        elif auth_type == "CREDENTIAL":
            # [{key:username,value:dc, description:desc},{key:password,value:pass, description:desc}]
            auth_credentials = response['data']['resource']['api_details']['api_source']['auth_credentials']  # - uname pwd
            uname_key = auth_credentials[0]['key']
            uname = auth_credentials[0]["value"]
            pwd_key = auth_credentials[1]['key']
            pwd = auth_credentials[1]["value"]
            header = {uname_key: uname, pwd_key: pwd}
    if auth_loc == "PARAM":
        if auth_type == "TOKEN":
            auth_token = response['data']['resource']['api_details']['api_source']['auth_token']
            param = {"access_token":auth_token}
        elif auth_type == "CREDENTIAL":
            auth_credentials = response['data']['resource']['api_details']['api_source']['auth_credentials'] #- uname pwd
            uname_key = auth_credentials[0]['key']
            uname = auth_credentials[0]["value"]
            pwd_key = auth_credentials[1]['key']
            pwd = auth_credentials[1]["value"]
            param = {uname_key: uname, pwd_key:pwd}
    response_type = response['data']['resource']['api_details']['response_type']
    try:
        api_request = requests.get(base_url + url_path, headers=header, params=param, verify=True)
    except:
        api_request = requests.get(base_url + url_path, headers=header, params=param, verify=False)
    api_response = api_request.text
    if response_type == "JSON":
        print("in if...")
        with open(file_name + "-data.json", 'w') as f:
            f.write(api_response)
        file_path = file_name + "-data.json"
    if response_type == "CSV":
        csv_data = StringIO(api_response)
        data = pd.read_csv(csv_data, sep=",")
        temp_file_name = uuid.uuid4().hex
        if p_id is not None:
            logger = log_utils.set_log_file(p_id, "api_resource_pipeline")
            logger.info("INFO: Received API resource with pre-saved pipeline details")
            if not data.empty:
                data.to_pickle(temp_file_name)
            pipeline_obj = Pipeline.objects.get(pk=p_id)
            pipeline_obj.dataset_id = response['data']['resource']['dataset']['id']
            pipeline_obj.save()
            transformed_data = task_executor(p_id, temp_file_name, "api_res", "")
        else:
            transformed_data = data
        if request_columns == "":
            column_selected_df = transformed_data
        else:
            column_selected_df = data.loc[:, data.columns.isin(request_columns.split(","))]
        # if row length is not specified return all rows
        if request_rows == "" or int(request_rows) > len(column_selected_df):
            final_df = column_selected_df
        else:
            num_rows_int = int(request_rows)
            final_df = column_selected_df.iloc[:num_rows_int]
        final_df.to_csv(file_name + "-data.csv")
        file_path = file_name + "-data.csv"
    status = "FETCHED"
    files = [
        ('0', (file_path, open(file_path, 'rb'), response_type))
    ]
    variables = {"file": None}

    map = json.dumps({"0": ["variables.file"]})

    file_upload_query = f"""
  mutation($file: Upload!) {{update_data_request(data_request: {{
  id: "{request_id}",
  status: {status},
  file: $file
  }}) {{
    success
    errors
    data_request {{
      id
      status
      file
    }}
  }}
}}"""
    print(file_upload_query)
    operations = json.dumps({
        "query": file_upload_query,
        "variables": variables
    })
    # headers = {}
    try:
        response = requests.post(graph_ql_url, data={"operations": operations, "map": map},
                                 files=files, headers=headers)
        print(response.text)
    except Exception as e:
        print(e)
    finally:
        files[0][1][1].close()
        os.remove(file_path)

