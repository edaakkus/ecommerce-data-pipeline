from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Ecommerce-Kafka-Spark-Streaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

print("Kafka DataFrame schema:")
df_kafka.printSchema()

print("Rows:", df_kafka.count())
print("Kafka source partitions:", df_kafka.rdd.getNumPartitions())

df_kafka_6 = df_kafka.repartition(6)

print("After Spark repartition:", df_kafka_6.rdd.getNumPartitions())

df_kafka_6.selectExpr(
    "CAST(value AS STRING)",
    "partition",
    "offset"
).show(truncate=False)

spark.stop()

