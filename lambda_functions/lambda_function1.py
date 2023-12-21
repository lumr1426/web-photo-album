import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

# Defining OpenSearch domain endpoint
host = 'OPENSEARCH_URL'
region = 'us-east-1'
service = 'es'
index = 'photos'
type = '_doc'
url = f'https://{host}/{index}/{type}'

# Set up the AWS4Auth instance
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def lambda_handler(event, context):
    
    # Use rekognition to get predicted labels
    rekognition = boto3.client('rekognition')
    s3 = boto3.client('s3')

    # Temporary s3 event for testing
    # s3_event = {
    #     'Records': [
    #         {
    #             'eventSource': 'aws:s3',
    #             'eventName': 'ObjectCreated:Put',
    #             's3': {
    #                 'bucket': {
    #                     "name": "b2-photos"
    #                 },
    #                 'object': {
    #                     'key': 'dog_image.jpg'
    #                 }
    #             },
    #             'eventTime': "2023-11-05T12:40:02"
    #         }
    #     ]
    # }

    # bucket name and the image name that triggered the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    image_name = event['Records'][0]['s3']['object']['key']
    created_time = event['Records'][0]['eventTime']

    # Detection using rekognition
    detect_response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_name
            }
        },
        MaxLabels=10,  # Maximum number of labels to detect
        MinConfidence=70
    )

    # The actual labels
    detected_labels = [label['Name'] for label in detect_response['Labels']]

    # The labels from the metadata (Will be stored by us I guess)
    image_metadata = s3.head_object(Bucket = bucket_name, Key = image_name)
    print(f'image_metadata is {image_metadata}')
    
    custom_labels_metadata_string = image_metadata.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amz-meta-customlabels', '')
    
    if custom_labels_metadata_string:
        custom_labels_metadata = custom_labels_metadata_string.split(',')

    print(f"custom labels are: {custom_labels_metadata}")

    # Combine labels detected and from metadata
    final_labels = detected_labels + custom_labels_metadata

    print(f'final labels are: {final_labels}')
    
    # Creating document for json
    document = {
        "objectKey": image_name,
        "bucket": bucket_name,
        "createdTimestamp": created_time,
        "labels": final_labels
    }

    # Indexing to opensearch
    send_to_opensearch(document)

    # Reached here, it is success
    return {
        'statusCode': 200,
        'body': json.dumps('Run Success')
    }


def send_to_opensearch(document):
    # Index the document
    try:
        response = requests.post(url, auth=awsauth, json=document, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        print(f'Successfully indexed the document: {response.text}')
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f'Failed to index document: {err}')
        raise err
