import json
import logging
import boto3
import pprint
import random
import ast
import urllib3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
REGION =  "us-east-1"


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_restaurant_by_cuisine_type(client, index, cuisine_type):
    res = client.search(index=index, body={'query': {'match': {'Cuisine': cuisine_type}}})
    # gets a random restaurant recommendation for the cuisine collected through
    # conversation from ElasticSearch and DynamoDB
    random_restaurant = random.choice(res['hits']['hits'])
    return random_restaurant["_source"]["Business_ID"]


def get_restaurant_by_id(table, id: str):
    '''retrieve the object using DynamoDB.Table.get_item()'''

    response = table.query(KeyConditionExpression=Key('Business_ID').eq(id))
    item = response['Items']
    return item[0]


def make_sms(cuisine_type: str, people_num, time, restaurant_name, restaurant_location):
    sms = "Hello! Here are my " + cuisine_type.capitalize() + " restaurant suggestion for " + people_num + " people, at " \
          + time + \
          ": " + restaurant_name + " at " + restaurant_location
    return sms




def lambda_handler(event, context):
    '''
    1. pull msg from SQS queue
    '''
    sqs = boto3.client('sqs',
                   aws_access_key_id= AWS_ACCESS_KEY,
                   aws_secret_access_key= AWS_SECRET_KEY,
                   region_name=REGION)

    response = sqs.receive_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/11010023967/Q1",
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    logger.debug(response)

    '''
    2. Gets a random restaurant from ElasticSearch & DynamoDB
    '''
    if "Messages" in response.keys():
        handler = response['Messages'][0]["ReceiptHandle"]
        sqs.delete_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/11011003967/Q1",
            ReceiptHandle=handler
        )

        data = response['Messages'][0]["Body"]
        data = ast.literal_eval(data)
        print(data)

        cuisine_type = data["Cuisine"]
        phone_number = data["Phone_number"]
        people_num = data["Number_of_people"]
        time = data["Dining_Time"]
        date = data["Dining_Date"]
        location = data["Location"]

        print(cuisine_type, phone_number, people_num,date, time, location)

        '''
        Elasticsearch
        https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-request-signing.html#es-request-signing-python
        '''
        awsauth = AWS4Auth(AWS_ACCESS_KEY,
                           AWS_SECRET_KEY,
                           REGION,
                           "es"
                        )
        eshost = "search-restaurants-ptymm4pztavgvv5khrjcp3gy.us-east-1.es.amazonaws.com"  #
        es = Elasticsearch(
                hosts=[{'host': eshost, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection ,
                port=433  # https
            )
        restaurant_id = get_restaurant_by_cuisine_type(es, "restaurants", cuisine_type)
        logger.debug(restaurant_id)

        '''
        DynamoDB
        '''
        session = boto3.Session(
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
        )
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('yelp-restaurants')

        restaurant = get_restaurant_by_id(table=table, id=restaurant_id)
        logger.debug(restaurant)

        '''
        3. Format msg
        '''
        ms = make_sms(cuisine_type, people_num, time, restaurant["name"], restaurant["address"])
        logger.debug(ms)

        '''
        4.send over text msg using SNS
        '''
        # Send your sms message
        client = boto3.client(
            "sns",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="us-east-1"
        )

        client.publish(
            PhoneNumber="+1" + str(phone_number),
            Message=ms
        )
