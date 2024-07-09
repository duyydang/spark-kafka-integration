# We have some Step to build that

## 1 Run docker compose
docker compose up -

## 2 Run spark master
$SPARK_HOME/sbin/start-master.sh

## 3Run submit spark spark-submit
--master spark://duydang:7077
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
spark_kafka_integration.py
