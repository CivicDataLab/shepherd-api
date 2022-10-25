import requests
import json

auth_url = "https://auth.idp.civicdatalab.in/users/get_sys_token"


def get_sys_token(func):
    def inner(*args, **kwargs):
        print("getting system token")

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