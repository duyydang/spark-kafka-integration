from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json, col
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Kafka connection details
kafka_bootstrap_servers = "localhost:9091,localhost:9092,localhost:9093"
kafka_topic = "test-url-1204"

# Define the schema for the JSON messages from Kafka
json_schema = StructType([
    StructField('sslsni', StringType(), True),
    StructField('subscriberid', StringType(), True),
    StructField('hour_key', IntegerType(), True),
    StructField('count', IntegerType(), True),
    StructField('up', IntegerType(), True),
    StructField('down', IntegerType(), True)
])

logger.info("Initializing Spark session")
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Kafka Spark Integration") \
    .getOrCreate()

# Set log level for streaming query manager
spark.sparkContext.setLogLevel("INFO")

logger.info(f"Using Kafka bootstrap servers: {kafka_bootstrap_servers}")
logger.info(f"Subscribing to Kafka topic: {kafka_topic}")

logger.info("Creating streaming DataFrame from Kafka")
# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("failOnDataLoss", "false") \
    .option("subscribe", kafka_topic) \
    .load()

logger.info("Processing Kafka messages")
# Convert value from Kafka (which is binary) to string
json_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse JSON data
parsed_df = json_df.withColumn("value", from_json(col("value"), json_schema)) \
    .select("value.*")

logger.info("Starting streaming query")
# Start the streaming query
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 10) \
    .start()

logger.info("Streaming query started. Awaiting termination.")
# Wait for the stream to finish
query.awaitTermination()
