# Twitter Sentiment Analysis with PySpark, Kafka, Tweepy and TextBlob

## Description 
This project seeks to simulate a real-time data ingestion and analytics pipeline using Apache Spark on Python (PySpark), Apache Kafka, Tweepy and TextBlob. Real-time ingestion of big data is made possible by Apache Spark and Kafka (acting as a queue). The Twitter API is used for accessing tweets and helping us determine the general sentiments of the tweeting population on a specific topic. 

## Outline 
1. Listening for any tweets that have a specific word or set of words in it and pushing it to Kafka stream or queue.
2. Leveraging the structured streaming capability in PySpark to consume the data on Kafka and make it available for analytics. 
3. Due to the limitation of queries to be performed on the Streaming data, an aggregation is performed and the data pushed to the Pandas API on Spark to enable plotting and any other analytics. 

## Drawback 
Due to the conversion to the Pandas API on Spark for analysis, the following conversion must be executed regualarly to update the data available for analysis
```py
final_result = spark.sql("select * from aggregates").toPandas() 
```

## Setup 
1. Install Kafka. Go to the oficial online page and download the binaries and follow the installation guide
2. Start Kafka and Zookeeper 
    * Change directory into the kafka base folder and Run the following in 2 different tabs of the command line and keep them running
        * `./bin/zookeeper-server-start.sh config/zookeeper.properties`
        * `./bin/kafka-server-start.sh config/server.properties`
3. Create the kafka topic to be used for the streaming by running the command below in the terminal
    * `./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kafka_tweets_stream --partitions 1 --replication-factor 1`
4. Install Spark by following the official guide. 
5. Start spark from the base directory, run `./sbin/start-master.sh` 
6. Create a virtual environment and install the requirements `pip install -r requirements.txt` 
7. To integrate and run pyspark with Jupyter-notebook, create the following environmental variables by adding this to `~/.bashrc` or `~/.bash_profile` and activate/refresh by running `source ~/.bashrc`
    export SPARK_HOME=/opt/local//spark

    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

    export PYSPARK_PYTHON=/usr/local/bin/python3

    export PYSPARK_DRIVER_PYTHON='jupyter'

    export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8889'

8. Run the following from the virtual environment to start the jupyter-notebook `pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0`
9. Open the browser and access jupyter-notebook on http://localhost:8889 
10. Run `StreamingTweets.ipynb`
11. Run `Twitter Sentiments with Spark and Tweeps.ipynb`




