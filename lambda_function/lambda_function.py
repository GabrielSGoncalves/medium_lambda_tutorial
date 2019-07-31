import json
from io import StringIO
import boto3
import os
import pandas as pd


def write_dataframe_to_csv_on_s3(dataframe, filename, bucket):
    """ Write a dataframe to a CSV on S3 """

    # Create buffer
    csv_buffer = StringIO()

    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep="\t")

    # Create S3 object
    s3_resource = boto3.resource("s3")

    # Write buffer to S3 object
    s3_resource.Object(bucket, f'{filename}').put(
        Body=csv_buffer.getvalue())


def lambda_handler(event, context):

    # Get variables from event
    BUCKET = event.get('bucket')
    KEY = event.get('file_key')
    OUTPUT = event.get('output_file')

    # Set client to get file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=BUCKET,
                                    Key=KEY)
    csv_file = response["Body"]

    # Load csv as a Pandas Dataframe
    df_fifa19 = pd.read_csv(csv_file, index_col=0, low_memory=False)

    # Group Clubs by Overall mean for player
    df_avg_overall_by_club = pd.DataFrame(df_fifa19.groupby('Club')[
        'Overall'].mean().sort_values(ascending=False)).round(2)

    # Save the Dataframe to the same S3 BUCKET
    write_dataframe_to_csv_on_s3(df_avg_overall_by_club, OUTPUT, BUCKET)

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
