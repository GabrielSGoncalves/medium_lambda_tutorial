import boto3
import json


credentials_json = json.load()


def invoke_lambda(bucket, file_key, output_file, group, column):
    """Invoke Lambda function using boto3

    Parameters:

    - bucket: Name of your S3 bucket
    - file_key: Name of your CSV file on the S3 bucket
    - output_file: Name for the output CSV file
    - group: Column to use as group in groupby
    - column: Column to aggregate in groupby
    """

    # Set Lambda Client with credentials
    boto3.setup_default_session(region_name='us-east-1')
    client = boto3.client(
        'lambda',
        aws_access_key_id=credentials_json.get('access_key_id'),
        aws_secret_access_key=credentials_json.get('secret_access_key')
    )

    # Dictionary to be posted on the lambda event with information provided
    # by the user command line call
    payload = {
        "bucket": bucket
        "file_key": file_key
        "output_file": output_file
        "group": group
        "column": column
    }

    print(payload)
    # print('Sample: {}'.format(vcf_file))

    response = client.invoke(
        FunctionName='vcf_orchestrator',
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload)
    )

    print(response.get('ResponseMetadata').get('RequestId'))
    print(f'\nRequestId:{response}')
    return response

invoke_lambda(BUCKET, FILE_KEY, OUTPUT_FILE, GROUP, COLUMN)
