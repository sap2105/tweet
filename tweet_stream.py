import tweepy
import json
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

#
#  host = "search-jask-tweetmap-hhk4izgywmbpwob2zah4fcdiry.us-west-2.es.amazonaws.com"
host = "search-twittermaps-l5iltsnatgslozfwthusv36tb4.us-west-2.es.amazonaws.com"
# host = "localhost"
# port = 9200
port = 443
# Authentication details. To  obtain these visit dev.twitter.com
consumer_key= 'Your Credentials'
consumer_secret='Your Credentials'
access_token='Your Credentials'
access_token_secret='Your Credentials'
#
es = Elasticsearch(
        hosts=[{'host': host, 'port': port}],
        use_ssl=True,
        # http_auth=awsauth,
        verify_certs=True,
        connection_class=RequestsHttpConnection
        )

print(es.info())

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_status(self, status):
        json_data = status._json
        user_info = json_data['user']
        if json_data['coordinates']:
            skimmed = {
                    'id': json_data['id'],
                    'time': json_data['timestamp_ms'],
                    'text': json_data['text'].lower().encode('ascii','ignore').decode('ascii'),
                    'coordinates': json_data['coordinates'],
                    'place': json_data['place']
                    }
            try:
                es.index(index='cloud_tweet', doc_type='twitter', body={
                    'content': json_data['text'].lower().encode('ascii','ignore').decode('ascii'),
                    'user': user_info['name'],
                    'user_id': user_info['id'],
                    'coordinates': json_data['coordinates']['coordinates'],
                    })
            except:
                print('ElasticSearch indexing failed')

            print(skimmed)
        return True

def on_error(self, status):
    print status

if __name__ == '__main__':
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = tweepy.Stream(auth, l)
    #filter for these terms in tweet text
    terms = [
        'elections', 'cloud computing', 'nyc', 'new year'
            ,'india','usa','dhoni','microsoft','apple'
            ,'hollywood','bollywood' ]
    #stream
    #stream.filter(None,terms)
    while True:
        try:
            stream.filter(track=terms)
        except:
            continue

