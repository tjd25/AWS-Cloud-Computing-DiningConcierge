import requests
import pprint
import json
import datetime
import boto3
from os import path
from boto3.dynamodb.conditions import Key, Attr

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


cuisine_type = "chinese"
location = "manhattan"

aws_access_key_id = ""
aws_secret_access_key = ""

URL = "https://api.yelp.com/v3/businesses/search"
yelp_id = ""
yelp_key= ""
offset = 0
HEADERS = {"Authorization": "Bearer "+ yelp_key,
                        'Content-Type': 'application/json'}


def add_restaurants(table, l_d):
    '''add new items to the table using DynamoDB.Table.put_item()'''
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html

    # convert empty strings into None
    for d in l_d:
        for key, value in d.items():
            if value == "":
                d[key] = None

    for d in l_d:
        print(d["id"])
        table.put_item(
            Item={
                "Business_ID": d["id"],
                "name": d["name"],
                "address": d["address"],
                "coordinates": str(d["coordinates"]),
                "review_count": d["review_count"],
                "rating": str(d["rating"]),
                "zip_code": d["zip_code"],
                "cuisine_type": cuisine_type,
                "insertedAtTimestamp": str(datetime.datetime.now()),
            }
        )


def get_restaurant_by_id(table, id: str):
    '''retrieve the object using DynamoDB.Table.get_item()'''
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html

    response = table.query(KeyConditionExpression=Key('Business_ID').eq(id))
    item = response['Items']
    return item[0]

def scrape_yelp_restaurants():

    l_d = []
    for offset in range(0, 1000, 50):
        PARAMS = {'location': location, "term": cuisine_type, "limit": 50, "offset": offset}

        r = requests.get(URL, params=PARAMS, headers=HEADERS)
        data = r.json()["businesses"]

        for d in data:
            # Requirements: Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code
            dict_record = {
                "id": d["id"],
                "name": d["name"],
                "address": d["location"]["address1"],
                "coordinates": d["coordinates"],
                "review_count": d["review_count"],
                "rating": d["rating"],
                "zip_code": d["location"]["zip_code"],
                "cuisine_type": cuisine_type
            }
            l_d.append(dict_record)

    for d in l_d:
        print(d)

    j_d = json.dumps(l_d)

    with open("yelp-restaurants-" + cuisine_type + ".json", 'w') as out:
        json.dump(j_d, out)


if __name__ == '__main__':
    '''Yelp API'''
    if not path.exists(r"yelp-restaurants-" + cuisine_type + ".json"):
        scrape_yelp_restaurants()
    '''
    Get the DynamoDB service resource and the table
    '''
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # dynamodb = boto3.resource('dynamodb')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')

    '''
    Store the restaurant records scraped into the table
    '''
    # with open(r"yelp-restaurants-" + cuisine_type + ".json", "r") as read:
    #     l_d = json.load(read)
    # l_d = json.loads(l_d)
    # add_restaurants(table, l_d)

    '''
    Use the DynamoDB table “yelp-restaurants” to fetch more information about the restaurants (restaurant name,
    address, etc.)
    '''
    get_restaurant_by_id(table, "6zH2Ih2Hw2wwf9ssIbZYRw")
