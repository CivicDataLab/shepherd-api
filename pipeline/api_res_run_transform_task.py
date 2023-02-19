import json
import os
import uuid
from io import StringIO

import pandas as pd
import requests
from background_task import background
import xmltodict
import dicttoxml

import log_utils
from datatransform.models import Pipeline
from access_token_decorator import get_sys_token
from configparser import ConfigParser
import os
from pipeline.model_to_pipeline import task_executor

config = ConfigParser()

config.read("config.ini")

graph_ql_url = os.environ.get(
    "GRAPH_QL_URL", config.get("datapipeline", "GRAPH_QL_URL")
)

@get_sys_token
def api_res_run_transform_task(
    p_id,
    api_source_id,
    api_data_params,
    access_token=None 
):
    errors = []
    print("2", api_source_id)
    print("____", p_id)
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
          auth_token_key
          headers
        }}
        apiparameter_set {{
            key
            default
        }}
      auth_required
      url_path
      response_type
      request_type
      format_loc
      format_key
    }}
  }}
}}
"""

    headers = {"Authorization": access_token}

    file_name = (
        "api_resource-" + str(uuid.uuid4().hex)[0:5]
    )  # name of the file to be uploaded
    request = requests.post(graph_ql_url, json={"query": query}, headers=headers)
    response = json.loads(request.text)
    print(response)
    base_url = response["data"]["resource"]["api_details"]["api_source"]["base_url"]
    url_path = response["data"]["resource"]["api_details"]["url_path"]
    # # headers = response['data']['api_source']['headers']
    auth_loc = response["data"]["resource"]["api_details"]["api_source"][
        "auth_loc"
    ]  # - header/param?
    auth_type = response["data"]["resource"]["api_details"]["api_source"][
        "auth_type"
    ]  # if token/uname-pwd

    request_type = response["data"]["resource"]["api_details"]["request_type"]
    format_loc = response["data"]["resource"]["api_details"]["format_loc"]
    format_key = response["data"]["resource"]["api_details"]["format_key"]

    param = {}
    header = {}
    if auth_type == "TOKEN":
        auth_token = response["data"]["resource"]["api_details"]["api_source"][
            "auth_token"
        ]
        auth_token_key = response["data"]["resource"]["api_details"]["api_source"][
            "auth_token_key"
        ]
    if auth_type == "CREDENTIAL":
        auth_credentials = response["data"]["resource"]["api_details"]["api_source"][
            "auth_credentials"
        ]  # - uname pwd
        uname_key = auth_credentials[0]["key"]
        uname = auth_credentials[0]["value"]
        pwd_key = auth_credentials[1]["key"]
        pwd = auth_credentials[1]["value"]

    if auth_loc == "HEADER":
        if auth_type == "TOKEN":
            header = {auth_token_key: auth_token}
        elif auth_type == "CREDENTIAL":
            header = {uname_key: uname, pwd_key: pwd}
    if auth_loc == "PARAM":
        if auth_type == "TOKEN":
            param = {auth_token_key: auth_token}
        elif auth_type == "CREDENTIAL":
            param = {uname_key: uname, pwd_key: pwd}

    if format_key and format_key != "":
        if format_loc == "HEADER":
            header.update({format_key: target_format})
        if format_loc == "PARAM":
            param.update({format_key: target_format})

    response_type = response["data"]["resource"]["api_details"]["response_type"]
    target_format = response["data"]["resource"]["api_details"]["default_format"]
    
    for each in response["data"]["resource"]["api_details"]["apiparameter_set"]:
        print("---each", each)
        param.update({each["key"]: each["default"]})    
    param.update(api_data_params)

    print(
                "----fetch", header, param, base_url, url_path, response_type, target_format
            )
    try:
        if request_type == "GET":
            try:
                api_request = requests.get(
                    base_url + url_path, headers=header, params=param, verify=True
                )
            except:
                api_request = requests.get(
                    base_url + url_path, headers=header, params=param, verify=False
                )
        elif request_type == "POST":
            api_request = requests.post(
                base_url + url_path, headers=header, params=param, body={}, verify=False
            )
        elif request_type == "PUT":
            api_request = requests.put(
                base_url + url_path, headers=header, params=param, verify=False
            )
        api_response = api_request.text
    except Exception as e:
        errors.append({"Success": False, "error": str(e)})

    response_type = target_format if target_format != "" else response_type
    
    if response_type.lower() == "json" and len(errors) == 0:
        temp_file_name = uuid.uuid4().hex + ".json"
        if p_id is not None:
            logger = log_utils.set_log_file(p_id, "api_resource_pipeline")
            logger.info("INFO: Received API resource with pre-saved pipeline details")
            json_object = json.dumps(api_response, indent=4)
            with open(temp_file_name, "w") as outfile:
                outfile.write(json_object)
            pipeline_obj = Pipeline.objects.get(pk=p_id)
            pipeline_obj.dataset_id = response['data']['resource']['dataset']['id']
            pipeline_obj.save()
            transformed_data = task_executor(p_id, temp_file_name, "api_res", "", "JSON")
            print("^^^^", type(transformed_data))
            if not isinstance(transformed_data, str):
                transformed_data = json.dumps(transformed_data)
        else:
            transformed_data = api_response

    if response_type.lower() == "csv" and len(errors) == 0:
        print(api_response)
        csv_data = StringIO(api_response)
        data = pd.read_csv(csv_data, sep=",")

        temp_file_name = uuid.uuid4().hex
        if p_id is not None:
            logger = log_utils.set_log_file(p_id, "api_resource_pipeline")
            logger.info("INFO: Received API resource with pre-saved pipeline details")
            if not data.empty:
                data.to_csv(temp_file_name)   #to_pickle(temp_file_name)
            pipeline_obj = Pipeline.objects.get(pk=p_id)
            pipeline_obj.dataset_id = response['data']['resource']['dataset']['id']
            pipeline_obj.save()
            transformed_data = task_executor(p_id, temp_file_name, "api_res", "", "CSV")
        else:
            transformed_data = data

    if response_type.lower() == "xml" and len(errors) == 0:

        temp_file_name = uuid.uuid4().hex + ".xml"
        if p_id is not None:
            logger = log_utils.set_log_file(p_id, "api_resource_pipeline")
            logger.info("INFO: Received API resource with pre-saved pipeline details")
            with open(temp_file_name, "w") as outfile:
                outfile.write(api_response)
            pipeline_obj = Pipeline.objects.get(pk=p_id)
            pipeline_obj.dataset_id = response['data']['resource']['dataset']['id']
            pipeline_obj.save()
            transformed_data = task_executor(p_id, temp_file_name, "api_res", "", "XML")
            data_dict = transformed_data
            transformed_data = dicttoxml.dicttoxml(data_dict)
            print("^^^^", type(transformed_data))
        else:
            transformed_data = api_response
            
    return transformed_data, response_type

        