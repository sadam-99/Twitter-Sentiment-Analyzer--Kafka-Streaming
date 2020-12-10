# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 3

import json
import os
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from textblob import TextBlob

Elastic_Search_OB = Elasticsearch(hosts=['localhost'], port=9200)


def main():
    '''
    Main functions that Starts the Kakfa consumer and consumes the the Tweets 
    from the Kafka Producer and Does the Sentiment Analysis using the TextBlob
    '''
    # Kafka consumer
    Tweets_Consumer = KafkaConsumer("Tweets_DATA_Streams", auto_offset_reset='earliest')

    for Tweet_Data in Tweets_Consumer:
        Tweet_JSON = json.loads(Tweet_Data.value)
        Tweet_Text = TextBlob(Tweet_JSON["text"])
        Polarity_VALUE = Tweet_Text.sentiment.polarity
        Predicted_Sentiment = ""
        if Polarity_VALUE > 0:
            Predicted_Sentiment = 'positive'
        elif Polarity_VALUE < 0:
            Predicted_Sentiment = 'negative'
        elif Polarity_VALUE == 0:
            Predicted_Sentiment = 'neutral'
        print("The Predicted sentiment for the Tweet:",Tweet_Text, "::-->")
        print(Predicted_Sentiment)

        #Adding the Tweets Text,Date, Predicted Sentiment, Author, Polarity into the Elastic search Body
        Elastic_SE_Body = {"Writer": Tweet_JSON["user"]["screen_name"],
                       "DATE": Tweet_JSON["created_at"],
                       "Tweet_TEXT": Tweet_JSON["text"],
                       "POLARITY": Polarity_VALUE,
                       "Pred_Sentiment": Predicted_Sentiment}

        try:
            Elastic_Search_OB.index(index="twitter_sentiment_analysis",
                     doc_type="_doc",
                     body=Elastic_SE_Body)

            print("Twitter data has been stored in elasticsearch in index")
        except Exception as excep:
            print(excep)

        print('\n')


if __name__ == "__main__":
    main()