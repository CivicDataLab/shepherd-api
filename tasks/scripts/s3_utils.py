
import boto3

import glob

s3_client = boto3.client('s3')


bucket_name = "generic-pipeline-data"

s3_obj_url = "https://generic-pipeline-data.s3.ap-south-1.amazonaws.com/"
def upload_result(path, recursive=False):
    if recursive:
        folder = glob.glob(path)
        for file in folder:
            s3_client.upload_file(
                Filename=file,
                Bucket=bucket_name,
                Key=file
            )
    else:
        key = path.split("/")[-1]
        s3_client.upload_file(
                Filename=path,
                Bucket=bucket_name,
                Key=key)
        with open(path, "rb") as f:
                s3_client.upload_fileobj(f, bucket_name, path)
        file_url = s3_obj_url + key
        return file_url