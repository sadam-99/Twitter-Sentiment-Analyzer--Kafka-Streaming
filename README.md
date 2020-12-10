# // Code Author:-
# // Name: Shivam Gupta
# // Net ID: SXG190040
# // CS 6350.001 - Big Data Management and Analytics - F20 Assignment 3

## Implementation of Sentiment Analysis in Python (Kafka, Elastic-Search, Kibana), KMeans Clustering, ALS Collboratice Filtering

I have used Databricks for this Homework as well

## Uploading the Input Files into DBFS(Fatabricks File System):
By Drag and Drop You can upload the Input Files into DBFS


## How to Start the Servers
Start ZooKeeper:
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Start kafka-Server:
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

Start Elastic-Search(Run as Admin):
cd C:\elastic_stack\elasticsearch-7.9.3-windows-x86_64\elasticsearch-7.9.3\bin>elasticsearch.bat

Start Kibana(Run as Admin):
cd C:\elastic_stack\kibana-7.2.0-windows-x86_64\kibana-7.2.0-windows-x86_64\bin>kibana.bat

## Compiling and Running the Code:
Question 1
## Sentiment Analysis using Kafka, Elastic Search, Kibana:

### Run Producer
Run command:- ``` python Sentiment_Analysis_Producer.py  ``` on UTD CS Linux Servers / Anaconda Prompt/Command Prompt

### Run Consumer
Run command:- ``` python Sentiment_Analysis_Consumer.py  ``` on UTD CS Linux Servers / Anaconda Prompt/Command Prompt
        
## How to Use and Run the Notebook Files(.ipynb):
Open the .ipynb files into Databricks and Run each Cell to get the output for Question 2 and Question 3.


# Note: 
Question 2 and Question 3
There are Files containing their respective Solutions:
```Question2_KMeans.ipynb```, ```Question3_ALS.ipynb```
The folder named ```Python Scripts``` contains their .py versions(Their Python Scripts) of the Solutions

## Results (Output Files:)
The folder named ```Output Files``` cointains their outputs of Each Question.


