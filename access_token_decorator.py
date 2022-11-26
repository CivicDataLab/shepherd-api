import requests
import json
from configparser import ConfigParser
import os

config = ConfigParser()

config.read("config.ini")

auth_url =  os.environ.get('AUTH_URL', config.get("datapipeline", "AUTH_URL"))


def get_sys_token(func):
    def inner(*args, **kwargs):
        print('----', "getting system token")

        headers = {}
        response = requests.get(auth_url, headers=headers)
        response_json = json.loads(response.text)
        if response_json["success"] == False:
            return {
                "Success": False,
                "error": response_json["error"],
                "error_description": response_json["error_description"],
            }

        kwargs["access_token"] = response_json["access_token"]
        return func(*args, **kwargs)

    return inner
