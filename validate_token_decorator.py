import json

import requests
from configparser import ConfigParser
import os
from django.http import JsonResponse, HttpResponse

config = ConfigParser()

config.read("config.ini")

auth_url = os.environ.get('AUTH_URL_FOR_SYS_VERIFY', config.get("datapipeline", "AUTH_URL_FOR_SYS_VERIFY"))
print(auth_url)

def request_to_server(body, server_url):
    bd = json.loads(body)
    print(bd)
    headers = {"Content-type": "application/json", "access-token": bd.get("access_token")}
    bd.pop("access_token", None)
    body = json.dumps(bd)
    print(auth_url + server_url)
    response = requests.request(
        "POST", auth_url + server_url, data=body, headers=headers
    )
    response_json = json.loads(response.text)
    print(response_json, "&&&&&&")
    return response_json


def validate_token_or_none(func):
    def inner(*args, **kwargs):
        username = None
        print(args[0].__dict__, "****")
        user_token = ""
        print("inside inner!!!!!")
        if hasattr(args[0], "META"):
            user_token = args[0].META.get("HTTP_AUTHORIZATION", "")
        else:
            user_token = args[1].context.META.get("HTTP_AUTHORIZATION", "")
        if user_token == "":
            print("Whoops! Empty user")
            return JsonResponse({"Success": False, "error": "Empty user token"}, safe=False)
        else:
            body = json.dumps({"access_token": user_token})
            try:
                response_json = request_to_server(body, "verify_user_token")
                if not response_json["success"]:
                    return JsonResponse({"Success": False, "error": "Invalid token"}, safe=False)
            except Exception as e:
                return JsonResponse({"Success": False, "error": str(e)}, safe=False)
        return func(*args, **kwargs)

    return inner
