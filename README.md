# Twitter Sentiment Analysis with PySpark, Kafka, Tweepy and TextBlob

## Description 
This project seeks to simulate a real-time data ingestion and analytics pipeline using Apache Spark on Python (PySpark), Apache Kafka, Tweepy and TextBlob. Real-time ingestion of big data is made possible by Apache Spark and Kafka (acting as a queue). The Twitter API is used for accessing tweets and helping us determine the general sentiments of the tweeting population on a specific topic. 

## Outline 
1. Listening for any tweets that have a specific word or set of words in it and pushing it to Kafka stream or queue.
2. Leveraging the structured streaming capability in PySpark to consume the data on Kafka and make it available for analytics. 
3. Due to the limitation of queries to be performed on the Streaming data, an aggregation is performed and the data pushed to the Pandas API on Spark to enable plotting and any other analytics. 

## Drawback 
Due to the conversion to the Pandas API on Spark for analytics, this conversation must be executed regualarly to update the data available for analysis
```
final_result = spark.sql("select * from aggregates").toPandas() 
```




