import boto3
import os
import pdb
import json
s3_client = boto3.client('s3')


bucket_name = "generic-pipeline-data"

s3_obj_url = "https://generic-pipeline-data.s3.ap-south-1.amazonaws.com/"


def upload_result(path, folder=None):
    key = path.split("/")[-1]
    if folder:
        s3_client.upload_file(path, bucket_name, folder + key)
        folder_url = s3_obj_url + folder
        return folder_url
    else:
        s3_client.upload_file(
                Filename=path,
                Bucket=bucket_name,
                Key=key)
        # with open(path, "rb") as file:
        #         s3_client.upload_fileobj(file, bucket_name, path)
        file_url = s3_obj_url + key
        return file_url


def upload_folder(folder_name:str, path_to_local_folder:str):
    # check if folder already exists

    # Get a list of all the keys in the S3 bucket
    result = s3_client.list_objects_v2(Bucket=bucket_name)
    print(result)
    # Extract the common prefixes, which represent the virtual folders in the S3 bucket
    #result = json.loads(result)
    folders = [content['Key'] for content in result.get("Contents")]
    # Extract the virtual folder names by filtering the key names that end with a "/" character
    print("folders in s3-----", folders)
    if folder_name in folders:
        local_files = []
        for path, subdirs, files in os.walk(path_to_local_folder):
            for name in files:
                local_files.append(os.path.join(path, name))
        print("folder exists already!!")
        for file in local_files:
            print(file)
            upload_result(file, folder_name)
    else:
        # create a new folder in s3
        s3_client.put_object(Bucket=bucket_name, Key=folder_name)
        local_files = []
        for path, subdirs, files in os.walk(path_to_local_folder):
            for name in files:
                local_files.append(os.path.join(path, name))
        for file in local_files:
            upload_result(file, folder_name)

upload_folder('boto_folder_test5/', '/home/ubuntu/test-folder')