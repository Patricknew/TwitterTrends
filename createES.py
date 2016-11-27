import json
import pandas as pd
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from key import access_key, secret_key

import sys
reload(sys)
sys.setdefaultencoding('UTF8')

# AWS host
host = "search-twittmap-etr35ieexhu5irovghg3pz4fwy.us-east-1.es.amazonaws.com"

# Connect to AWS
awsauth = AWS4Auth(access_key, secret_key, 'us-east-1', 'es')

# Elasticsearch client
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# # Read in file
# tweets_data = []
# with open("tweet_data.txt", "r") as tweetfile:
#     for line in tweetfile.readlines():
#         tweet = json.loads(line)
#         tweets_data.append(tweet)
        
# tweets = pd.DataFrame()
# tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
# tweets['location'] = map(lambda tweet: tweet['coordinates']['coordinates'], tweets_data)


mappings = {"mappings":{
                "twitter": {
                    "properties": {
                         "tweet":  {
                            "type": "string"
                         },
                         "sentiment": {
                            "type": "string"
                         },
                         "location": {
                             "type": "geo_point"
                         }}
                }
            }}
es.indices.create(index='snsindex', body=mappings)

# ... 
#es_entries['geo'] = { 'location': str(data['_longitude_'])+","+str(data['_latitude_'])}
# ...
#es.index(index="geodata", doc_type="doc", body=es_entries)



# for i in range(len(tweets)):
#     es_entries = { 'tweet': tweets['text'][i],
#                    'location': str(tweets['location'][i][1])+","+str(tweets['location'][i][0])
#     }
#     es.index(index="twittmap", doc_type="twitter", body=es_entries)
#    post = {
#            'tweet': tweets['text'][i],
#            'coordinates': tweets['location'][i]
#    }
#    es.index(index="index",
#                    doc_type="twitter",
#                    body=post)
    
    
    #res = es.index(index="test-index", doc_type='tweet', id=json_obj['id'], body=json_obj)
