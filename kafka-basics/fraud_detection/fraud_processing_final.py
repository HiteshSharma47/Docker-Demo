from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

users_df = spark.read.csv("data/user_table.csv", header=True, inferSchema=True)

tx_schema = StructType([
    StructField("tx_id", IntegerType(), True),
    StructField("userId", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", DoubleType(), True),
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fraud_detection") \
    .load()

parsed_stream = kafka_stream.select(from_json(col('value').cast('string'), tx_schema).alias("tx")).select("tx.*")
fraud_stream = parsed_stream.filter(col('amount') > 10000.0)
normal_stream = parsed_stream.filter((col('amount') > 5000.0) & (col('amount') < 10000.0))

enriched_data1 = fraud_stream.join(users_df, "userId")
enriched_data2 = normal_stream.join(users_df, "userId")

output_stream2 = enriched_data2 \
    .withColumn("value", to_json(struct("*")).cast("string")) \
    .select("value")

output_stream1 = enriched_data1 \
    .withColumn("value", to_json(struct("*")).cast("string")) \
    .select("value")

query1 = output_stream1.writeStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "kafka:9092") \
 .option("topic", "fraud_notification") \
 .option("checkpointLocation", "/workspace/checkpoints12") \
 .start()

query2 = output_stream2.writeStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "kafka:9092") \
 .option("topic", "normal_notification") \
 .option("checkpointLocation", "/workspace/checkpoints23") \
 .start()

query1.awaitTermination()
query2.awaitTermination()