from datetime import datetime, timedelta

import boto3
import pytz
from prefect import flow

utc = pytz.UTC

default_days = 15


@flow
def delete_s3_files(days=default_days):
    """ Delete all files in the s3 that are older than 15 days."""
    date_to_delete = datetime.now().date() - timedelta(days=days)
    s3_client = boto3.client('s3')
    bucket_name = "generic-pipeline-data"
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    keys_to_delete = [{'Key': object['Key']}
                      for object in response['Contents']
                      if object['LastModified'].replace(tzinfo=None) < datetime(date_to_delete.year, date_to_delete.month, date_to_delete.day)]

    s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': keys_to_delete})
    return 1

if __name__ == "__main__":
    result = delete_s3_files()
    print(result)