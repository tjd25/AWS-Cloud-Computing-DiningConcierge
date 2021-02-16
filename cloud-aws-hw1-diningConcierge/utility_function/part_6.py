from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json
import random
import requests
import pprint


cuisine_type = "chinese"
# AWS credentials
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
# ElasticSearch Config
host = "search-restaurants-ptymm4piztavgvv5khrjcp3g3y.us-east-1.es.amazonaws.com"  #
region = "us-east-1"
service = 'es'

def add_restaurants_to_index(es, index, l_d):
    '''
    Store partial information for each restaurant scraped in ElasticSearch under the “restaurants” index, where each
    entry has a “Restaurant” data type. This data type will be of composite type stored as JSON in ElasticSearch.
    '''
    # https://elasticsearch-py.readthedocs.io/en/master/
    for d in l_d:
        d = {
            "Business_ID": d["id"],
            "Cuisine": d["cuisine_type"],
        }
        print(d)
        # Create an ElasticSearch type under the index “restaurants” called “Restaurant”
        es.index(index=index, doc_type="Restaurant", body=d)


def es_get_restaurant_by_cuisine_type(es, index, cuisine_type):
    res = es.search(index=index, body={'query': {'match': {'Cuisine': cuisine_type}}})
    print("Got %d Hits:" % res['hits']['total']['value'])
    print(res['hits']['hits'])
    for hit in res['hits']['hits']:
        print(hit)
        break
    # gets a random restaurant recommendation for the cuisine collected through conversation from ElasticSearch and
    # DynamoDB
    random_restaurant = random.choice(res['hits']['hits'])
    print(random_restaurant["_source"]["Business_ID"])


def get_restaurant_by_cuisine_type(url, cuisine_type):
    r = requests.get(url + cuisine_type)
    print(r)
    l_d = r.json()["hits"]["hits"]
    random_restaurant = random.choice(l_d)

    print(random_restaurant)
    random_restaurant_id = random_restaurant["_source"]["RestaurantID"]
    print(random_restaurant_id)
    return random_restaurant_id


if __name__ == '__main__':
    credentials = boto3.Session().get_credentials().get_frozen_credentials()
    awsauth = AWS4Auth(AWS_ACCESS_KEY,
                       AWS_SECRET_KEY,
        region, service, session_token=credentials.token)

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection ,
        port=433  # https
    )

    # load json data
    with open(r"yelp-restaurants-" + cuisine_type + ".json", "r") as read:
        l_d = json.load(read)
    l_d = json.loads(l_d)

    # add_restaurants_to_index(es=es, index="restaurants", l_d=l_d)

    es_get_restaurant_by_cuisine_type(es, "restaurants", cuisine_type)
    # get_restaurant_by_cuisine_type(
    #     url="https://search-restaurants-ptymm4piztavgvv5khrjcp3g3y.us-east-1.es.amazonaws.com/restaurants"
    #         "/_search?q=Cuisine:",
    #     cuisine_type= cuisine_type )

    # res = es.search(index="restaurants", body={"query": {"match_all": {}}})

    # res = es.search(index='restaurants', body={'query': {'match': {'Cuisine': 'chinese'}}})
    # print("Got %d Hits:" % res['hits']['total']['value'])
    # for hit in res['hits']['hits']:
    #     print(hit)
