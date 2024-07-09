# This project is very easy to use for newbie

## We have some Step to build this
### 1 Run docker compose
Open terminal and run: docker compose up -d \
In this docker-compose.yml file, I use kafdrop that will manage and monitor all messages and topics from kafka \
Additionally, you can uncomment service "spark" to use same environment with kafka

### 2 Run spark master
$SPARK_HOME/sbin/start-master.sh \
You need install kafka before run that. Maybe I will build a introduce next time ( Or you can search ) \
https://spark.apache.org/downloads.html

### 3 Run submit spark
You need check master spark in spark UI and replace \
--master <spark://duydang:7077> \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
spark_kafka_integration.py
