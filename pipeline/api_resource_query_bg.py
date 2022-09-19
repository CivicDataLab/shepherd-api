import json

import requests
from background_task import background

from datatransform.models import Pipeline


@background
def api_resource_query_task(p_id, api_source_id):
    print(api_source_id)
    pipeline_object = Pipeline.objects.get(pk=p_id)
    query =  f"""{{
  api_resource(api_resource_id: {api_source_id}) {{
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
      remark
      funnel
      action
      access_type
      License
      }}
    url_path
    api_source {{
      id
      title
      base_url
      description
      api_version
      headers
      auth_loc
      auth_type
      auth_credentials
      auth_token
      apiresource_set
      {{
        id
        title
        description
      }}
    }}
    auth_required
    response_type
    }}
  }}
"""
    headers = {}
    request = requests.post('https://idpbe.civicdatalab.in/graphql', json={'query': query}, headers=headers)
    response = json.loads(request.text)
    base_url = response['data']['api_resource']['api_source']['base_url']
    url_path = response['data']['api_resource']['url_path']
    # # headers = response['data']['api_source']['headers']
    auth_loc = response['data']['api_resource']['api_source']['auth_loc'] #- header/param?
    auth_type = response['data']['api_resource']['api_source']['auth_type']  #if token/uname-pwd
    param = {}
    header = {}
    if auth_loc == "HEADER":
        if auth_type == "TOKEN":
            auth_token = response['data']['api_source']['auth_token']
            header = {"access_token":auth_token}
        elif auth_type == "CREDENTIAL":
            # [{key:username,value:dc, description:desc},{key:password,value:pass, description:desc}]
            auth_credentials = response['data']['api_source']['auth_credentials']  # - uname pwd
            uname_key = auth_credentials[0]['key']
            uname = auth_credentials[0]["value"]
            pwd_key = auth_credentials[1]['key']
            pwd = auth_credentials[1]["value"]
            header = {uname_key: uname, pwd_key: pwd}
    if auth_loc == "PARAM":
        if auth_type == "TOKEN":
            auth_token = response['data']['api_source']['auth_token']
            param = {"access_token":auth_token}
        elif auth_type == "CREDENTIAL":
            auth_credentials = response['data']['api_source']['auth_credentials'] #- uname pwd
            uname_key = auth_credentials[0]['key']
            uname = auth_credentials[0]["value"]
            pwd_key = auth_credentials[1]['key']
            pwd = auth_credentials[1]["value"]
            param = {uname_key: uname, pwd_key:pwd}
    api_request = requests.get(base_url + "/" + url_path, headers=header, params=param)
    api_response = json.loads(api_request.text)
    # pipeline_object.status = "Done"
