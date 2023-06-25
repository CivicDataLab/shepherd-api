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


def json_keep_column(data, cols, parentnodes):
    print ('------------inkeepcol', parentnodes)
    
    try:
        
        def get_child_keys(d, child_keys_list):
            if isinstance(d,  dict):
                for key in list(d.keys()):
                    child_keys_list.append(key)
                    #if isinstance(d[key], dict):
                    get_child_keys(d[key], child_keys_list)  
            if isinstance(d,  list):
                for each in d:
                    if isinstance(each,  dict):
                        get_child_keys(each, child_keys_list)                          

        def remove_a_key(d, parent, remove_key, parent_dict):
            for key in list(d.keys()):
                child_keys_list = []
                get_child_keys(d[key], child_keys_list)
                #print ('--------------', key, '---', child_keys_list)
                #if (key.lower() not in remove_key or parent.lower()!=parent_dict.get(key, "").lower()) and not any([ item.lower() in remove_key for item in child_keys_list]):
                if (key.lower() not in remove_key) and not any([ item.lower() in remove_key for item in child_keys_list]):
                    del d[key]
                else:
                    keep_col(d[key], key, remove_key, parent_dict)

        def keep_col(d, parent, remove_key, parent_dict):
            if isinstance(d, dict):
                remove_a_key(d, parent, remove_key, parent_dict)
            if isinstance(d, list):
                for each in d:
                    if isinstance(each, dict):
                        remove_a_key(each, parent, remove_key, parent_dict)
            return d
        

        parent_dict = {}

        for each in parentnodes:
            node_path = [x for x in each.split('.') if x != "" and x != "." and "items" not in x]
            parent_dict[node_path[-1]] = node_path[-2] if len(node_path)>=2 else ""
        cols = [x.lower() for x in cols]
        return keep_col(data, "", cols, parent_dict)
    except Exception as e:
        print ('-----', str(e))
        return data



@get_sys_token
def api_resource_query_task(
    p_id,
    api_source_id,
    request_id,
    request_columns,
    remove_nodes,
    request_rows,
    target_format,
    access_token=None 
):
    errors = []
    print("2", api_source_id)
    request_id = str(request_id)
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
    data_request_query = f""" 
        {{
        data_request(data_request_id: "{request_id}") {{
            id
            status
            resource {{
                id
                schema_exists
            }}
            parameters
        }}
    }}
    """
    print(data_request_query)
    headers = {"Authorization": access_token}

    get_datarequest_details = requests.post(
        graph_ql_url, json={"query": data_request_query}, headers=headers
    )
    datarequest_response = json.loads(get_datarequest_details.text)
    print(datarequest_response, "-------")
    data_request_parameters = datarequest_response["data"]["data_request"]["parameters"]
    print(type(data_request_parameters), "????")
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
    param.update(json.loads(data_request_parameters))
    print(
        "final params.........................................$$$$",
        request_type,
        param,
        headers,
        base_url,
        url_path,
        target_format,
        response_type,
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
    # format_changed_file = (
    #     ""  # holds the  filename if change_format transformation is applied
    # )
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
        print("--------datafromapi", api_request)
        # data = api_request.json()
        # print("--------------------jsonparse", data, "----", request_columns)
        if len(request_columns) > 0:
            # filtered_data = json_keep_column(data, request_columns)
            transformed_data = json.loads(transformed_data) if isinstance(transformed_data, str) else transformed_data
            filtered_data = json_keep_column(transformed_data, request_columns, remove_nodes)
            print("-----------------fltrddata", filtered_data)
        else:
            filtered_data = transformed_data
        with open(file_name + "-data.json", "w") as f:
            json.dump(filtered_data, f)
            # f.write(str(filtered_data))
            file_path = file_name + "-data.json"
    if response_type.lower() == "csv" and len(errors) == 0:
        print(api_response)
        csv_data = StringIO(api_response)
        data = pd.read_csv(csv_data, sep=",", index_col=False)
        print("------------- parsed csv")
        print(data)
        print("------", request_columns , '----')
        temp_file_name = uuid.uuid4().hex
        if p_id is not None:
            logger = log_utils.set_log_file(p_id, "api_resource_pipeline")
            logger.info("INFO: Received API resource with pre-saved pipeline details")
            if not data.empty:
                data.to_csv(temp_file_name, index=False)   #to_pickle(temp_file_name)
            pipeline_obj = Pipeline.objects.get(pk=p_id)
            pipeline_obj.dataset_id = response['data']['resource']['dataset']['id']
            pipeline_obj.save()
            transformed_data = task_executor(p_id, temp_file_name, "api_res", "", "CSV")
        else:
            transformed_data = data
        if request_columns == []:
            column_selected_df = transformed_data
        else:
            cols = list(transformed_data.columns.values)
            if not (set(cols) & set(request_columns)):
                errors.append(
                    {
                        "Success": False,
                        "error": "requested columns not found in data header ",
                    }
                )
            else:
                request_columns = [x.lower() for x in request_columns]
                transformed_data.columns = [x.lower() for x in transformed_data.columns]  
                column_selected_df = transformed_data.loc[
                    :, transformed_data.columns.isin(request_columns)
                ]
        # if row length is not specified return all rows
        if len(errors) == 0:
            if request_rows == "" or int(request_rows) > len(column_selected_df):
                final_df = column_selected_df
            else:
                num_rows_int = int(request_rows)
                final_df = column_selected_df.iloc[:num_rows_int]
            final_df.to_csv(file_name + "-data.csv", index=False)
            file_path = file_name + "-data.csv"
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
            print("^^^^", type(transformed_data))
        else:
            data_dict = xmltodict.parse(api_response)
        #print("--------datafromapi", transformed_data)
        
        
        print("-----------dict", data_dict, "-----------", request_columns)
        if len(request_columns) > 0:
            filtered_data = json_keep_column(data_dict, request_columns, remove_nodes)
        else:
            filtered_data = data_dict
        print("----datafltrd", filtered_data)
        xml_data = dicttoxml.dicttoxml(data_dict)
        print("-----xml", xml_data)
        with open(file_name + "-data.xml", "w") as f:
            f.write(xml_data.decode())
        file_path = file_name + "-data.xml"
    if response_type.lower() not in ["xml", "csv", "json"] and len(errors) == 0:
        with open(file_name + "-data.xml", "w") as f:
            f.write(api_response)
        file_path = file_name + "-data.xml"
    status = "FETCHED"
    if len(errors) > 0:
        with open(file_name + "-error.txt", "w") as f:
            json.dump(errors, f)
        file_path = file_name + "-error.txt"
    files = [("0", (file_path, open(file_path, "rb"), response_type))]
    print("uploading....&&&&", files)
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
    operations = json.dumps({"query": file_upload_query, "variables": variables})
    # headers = {}
    try:
        response = requests.post(
            graph_ql_url,
            data={"operations": operations, "map": map},
            files=files,
            headers=headers,
        )
        print(response.text)
    except Exception as e:
        print(e)
    finally:
        files[0][1][1].close()
        # os.remove(file_path)
