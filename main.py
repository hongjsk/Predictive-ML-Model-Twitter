
import tweepy
import sys, json
import pandas as pd
import csv
import os
import types
from botocore.client import Config
import ibm_boto3

#Twitter API credentials
consumer_key = <"YOUR_CONSUMER_API_KEY">
consumer_secret = <"YOUR_CONSUMER_API_SECRET_KEY">
screen_name = "@CharlizeAfrica"  #you can put your twitter username, here we are using Charlize Theron twitter profile to analyze.

def main(dict):
    tweets = get_all_tweets()
    createFile(tweets)

    return {"message": 'success' }

def get_all_tweets():
    # initialize tweepy
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth)

    alltweets = []
    for status in tweepy.Cursor(api.user_timeline, screen_name = screen_name).items(3200):
        alltweets.append(status)

    return alltweets

def createFile(tweets):
    outtweets=[]
    for tweet in tweets:
        outtweets.append([tweet.created_at.hour,
                          tweet.text, tweet.retweet_count,
                          tweet.favorite_count])

    client = ibm_boto3.client(service_name='s3',
    ibm_api_key_id=<"COS_API_KEY">,
    ibm_service_instance_id= <"COS_SERVICE_ID">,

    config=Config(signature_version='oauth'),
    endpoint_url= "https://" + <"COS_ENDPOINT_URL">)

    cols=['hour','text','retweets','favorites']
    table=pd.DataFrame(columns= cols)

    for i in outtweets:
        table=table.append({'hour':i[0], 'text':i[1], 'retweets': i[2], 'favorites': i[3]}, ignore_index=True)
    table.to_csv('tweets_data.csv', index=False)

    try:
        res=client.upload_file(Filename="tweets_data.csv", Bucket=<'BUCKET_NAME'>,Key='tweets.csv')
    except Exception as e:
        print(Exception, e)
    else:
        print('File Uploaded')