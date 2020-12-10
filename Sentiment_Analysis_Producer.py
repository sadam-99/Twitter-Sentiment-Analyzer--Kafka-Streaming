# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 3

import json
import tweepy
import kafka
from kafka import KafkaProducer
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler


# My TWITTER API CONFIGURATIONS
API_KEY = "zvsP3Wdpc6MMIoy8Hkvwpsxf1"
API_SECRET_KEY = "UdaAkdJVKxGq5r0uycfzW7EMobTJWQcQXOBy0fAk78fAA9Lq4U"
ACCESS_TOKEN = "3693072796-ABQtgAFJQxmnX7Q9ubhuc5pfd342i7m1T3B4i0o"
ACCESS_SECRET_TOKEN = "QRl3KeuozzntLIDGByQxSmJjEtVT3ZES8w5IVAua2ihbg"

# TWITTER API Authentication
Twitter_API_AUTH = OAuthHandler(API_KEY, API_SECRET_KEY)
Twitter_API_AUTH.set_access_token(ACCESS_TOKEN, ACCESS_SECRET_TOKEN)

Twitter_API = tweepy.API(Twitter_API_AUTH)


# Twitter Stream Listener
class Kafka_Stream_Listener(StreamListener):
    def __init__(self):
        # localhost:9092 = This is the Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        # kafka Producer will fetch the Tweets data from the Twitter and provide it to Kafka Consumer
        self.producer.send("Tweets_DATA_Streams", data.encode('utf-8'))
        print(data)
        return True

    def Any_Error(self, status):
        print(status)
        return True


# Twitter Data Stream
Tweets_Data_Stream = Stream(Twitter_API_AUTH, Kafka_Stream_Listener())

# Producing the Tweets Data with hashtags: #trump', '#coronavirus' hashtags
Tweets_Data_Stream.filter(track=['#trump', '#coronavirus'])