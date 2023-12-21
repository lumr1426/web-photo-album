import json
import boto3
import argparse
import logging
import requests
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger(__name__)

REGION = 'us-east-1'
HOST = 'S3_URL'
INDEX = 'photos'


def generate_presigned_url(s3_client, client_method, method_parameters, expires_in):
    """
    Generate a presigned Amazon S3 URL that can be used to perform an action.

    :param s3_client: A Boto3 Amazon S3 client.
    :param client_method: The name of the client method that the URL performs.
    :param method_parameters: The parameters of the specified client method.
    :param expires_in: The number of seconds the presigned URL is valid for.
    :return: The presigned URL.
    """
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod=client_method, Params=method_parameters, ExpiresIn=expires_in
        )
        logger.info("Got presigned URL: %s", url)
    except ClientError:
        logger.exception(
            "Couldn't get a presigned URL for client method '%s'.", client_method
        )
        raise
    return url


def get_lex_results(query):
    
    # create lex runtime
    lex_runtime = boto3.client('lex-runtime')
    
    bot_name = 'photo_album'
    bot_alias = 'test'
    
    # send the user input to lex and get response
    response = lex_runtime.post_text(
            botName = bot_name,
            botAlias = bot_alias,
            userId = '200',
            inputText = query
        )
    
    # parse slot from the response
    slots = response.get('slots')
    
    if slots is None: return []
    
    keywords = []
    
    for value in slots.values():
        if value is not None:
            keywords.append(value)
        
    # if none were caught, return empty array
    if len(keywords) == 0:
        return []
    else: return keywords


def query(term):
    q = {'size': 10, 'query': {'function_score': {
                'query': {'multi_match': {'query': term, 'fields': ['labels']}},
                'random_score': {},
            }
        }
    }

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    seen = set()
    result = []
    for dic in results:
        if dic['objectKey'] not in seen:
            seen.add(dic['objectKey'])
            result.append(dic)

    return result


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)



def lambda_handler(event, context):
    
    search_query = event['queryStringParameters']['q']
    
    #temp_query = 'show me birds'
    
    # send the query to lex and get keywords
    keywords = get_lex_results(search_query)

    print(f'lex output keywords are: {keywords}')

    if not keywords: return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True},
        "body": json.dumps({'list': []})
    }
    
    final_urls = []
    
    print(f"query result is: {query(keywords[0])}")
    
    s3_client = boto3.client('s3')
    client_method = 'get_object'
    
    # append search results to the list
    for keyword in keywords:
        
        if not keyword: continue
        
        query_result = query(keyword)
        
        if not query_result: continue
        
        keyword_urls = []

        # Loop through query results for the current keyword
        for result in query_result:
            bucket_name = result['bucket']
            key = result['objectKey']
            method_parameters = {"Bucket": bucket_name, "Key": key}
    
            # Generate and append the URL for the current result
            url = generate_presigned_url(s3_client, client_method, method_parameters, 1000)
            keyword_urls.append(url)
            
        final_urls.append(keyword_urls)
    
    if not final_urls[0]: return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True},
        "body": json.dumps({'list': []})
    }

    # Note: all RESPONSE need to be in the following format, note that the BODY needs to be STRINGNIFIED 
    ret = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True},
        "body": json.dumps({'list': final_urls[0]})
    }
    return ret
