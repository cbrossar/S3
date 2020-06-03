import boto3
from config import path_to_aws_credentials


def get_s3_client():

    credentials = get_local_aws_credentials()

    client = boto3.client('s3',
                          aws_access_key_id=credentials['aws_access_key_id'],
                          aws_secret_access_key=credentials['aws_secret_access_key'],
                          aws_session_token=credentials['aws_session_token'],
                          region_name=credentials['region'])

    return client


def get_local_aws_credentials():

    d = dict()
    with open(path_to_aws_credentials) as f:
        for line in f:
            if len(line.split()) != 3:
                continue
            (key, equals, val) = line.split()
            d[key] = val

    credentials = dict(region=d['region'],
                       aws_access_key_id=d['aws_access_key_id'],
                       aws_secret_access_key=d['aws_secret_access_key'],
                       aws_session_token=d['aws_session_token'])
    return credentials
