from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, format_string, round
import time

def check_kafka_topic(logger, broker_url="kafka:9092", topic_name = "weather_data"):
    from kafka import KafkaAdminClient
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker_url)
        topic_list = admin_client.list_topics()
        admin_client.close()
        if topic_name in topic_list:
            logger.info(f"Topic '{topic_name}' exists in Kafka broker '{broker_url}'.")
            return True
        else:
            logger.warn(f"Topic '{topic_name}' does NOT exist in Kafka broker '{broker_url}'.")
            return False
    except Exception as e:
        logger.error(f"Kafka broker '{broker_url}' is NOT available. Error: {str(e)}")
        return False
# Schema cho dữ liệu 
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("base", StringType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("feels_like", DoubleType()),
        StructField("temp_min", DoubleType()),
        StructField("temp_max", DoubleType()),
        StructField("pressure", IntegerType()),
        StructField("humidity", IntegerType()),
        StructField("sea_level", IntegerType()),
        StructField("grnd_level", IntegerType())
    ])),
    StructField("visibility", IntegerType()),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg", IntegerType()),
        StructField("gust", DoubleType())
    ])),
    StructField("rain", StructType([
        StructField("1h", DoubleType())
    ]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType())
    ])),
    StructField("dt", LongType()),
    StructField("sys", StructType([
        StructField("type", IntegerType()),
        StructField("id", IntegerType()),
        StructField("country", StringType()),
        StructField("sunrise", LongType()),
        StructField("sunset", LongType())
    ])),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType())
])

# Các file jar cần thiết
jars = [
        "/streaming/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "/streaming/jars/kafka-clients-3.5.0.jar",
        "/streaming/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
        "/streaming/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar",
        "/streaming/jars/commons-pool2-2.12.0.jar",
    ]

# Khởi tạo spark session
def initial_spark():
    from pyspark.sql import SparkSession
    spark =  SparkSession.builder \
        .appName("Transform Process") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", '4G') \
        .config("spark.executor.cores", '4') \
        .config("spark.executor.instances", "1") \
        .config("spark.cores.max", '4') \
        .config("spark.deploy.defaultCores", "4")   \
        .config("spark.task.cpus", "4") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .config("spark.executorEnv.TZ", "Asia/Ho_Chi_Minh") \
        .config("spark.hadoop.fs.defaultFS","hdfs://namenode:9000") \
        .getOrCreate()
    
    # Lấy Spark logger
    sc = spark.sparkContext
    sc.setLogLevel("INFO") 
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    
    logger.info("Create Spark Session Successfully")
    return spark, logger

# Hàm xử lý dữ liệu
def spark_process(spark,logger):
    try:
        logger.info("Start Transform Processing...")
        df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:9092") \
                    .option("subscribe", "weather_data") \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .load()

        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

        # Chuyển đổi và làm phẳng dữ liệu
        processed_df = parsed_df.select(
            col("id").alias("city_id"),
            col("name").alias("city_name"),
            format_string("%.2f", col("coord.lat")).cast(DoubleType()).alias("latitude"),
            format_string("%.2f", col("coord.lon")).cast(DoubleType()).alias("longitude"),
            to_timestamp(col("dt")).alias("timestamp"),
            col("weather")[0]["main"].alias("weather_condition"),
            col("weather")[0]["description"].alias("weather_description"),
            round(col("main.temp") - 273.15).alias("temperature_celsius"),
            round(col("main.feels_like") - 273.15).alias("feels_like_celsius"),
            round(col("main.temp_min") - 273.15).alias("temp_min_celsius"),
            round(col("main.temp_max") - 273.15).alias("temp_max_celsius"),
            col("main.pressure").alias("pressure_hpa"),
            col("main.humidity").alias("humidity_percent"),
            col("wind.speed").alias("wind_speed_mps"),
            col("wind.deg").alias("wind_direction_deg"),
            col("wind.gust").alias("wind_gust_mps"),
            col("rain.1h").alias("rain_1h_mm"),
            col("clouds.all").alias("cloud_coverage_percent"),
            col("sys.country").alias("country_code"),
            to_timestamp(col("sys.sunrise")).alias("sunrise_time"),
            to_timestamp(col("sys.sunset")).alias("sunset_time"),
            (col("timezone") / 3600).alias("timezone_hours"),
            date_format(to_timestamp(col("dt")), "yyyy-MM-dd").alias("date"),
        )
        # Xử lý các dữ liệu trùng lặp 
        processed_df = processed_df.withWatermark("timestamp", "10 minutes").dropDuplicates(["city_id","timestamp"])
        # Điền các dữ liễu khuyết
        processed_df = processed_df.fillna({
            "weather_condition": "Unknown",
            "temperature_celsius": 0.0,
            "humidity_percent": 0,
            "wind_speed_mps": 0.0, 
            "wind_gust_mps": 0.0,
            "rain_1h_mm": 0.0,
            "cloud_coverage_percent": 0
        })
        # Lưu dữ liệu lên HDFS với partition theo ngày và giờ
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "hdfs://namenode:9000/data/weather") \
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/weather") \
            .partitionBy("date","city_name") \
            .start()
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in spark_process: {str(e)}")
        raise
    

if __name__ == "__main__":
    max_retries = 20
    spark,logger = initial_spark()
    try:
        for attempt in range(max_retries):
            if check_kafka_topic(logger=logger):
                try:
                    spark_process(spark=spark,logger=logger)
                    break
                except Exception as e:
                    logger.error("Getting error waiting 5s to retry...")
                    time.sleep(5)
                    if attempt == max_retries - 1:
                        raise
            else:
                logger.error("Cant connect to Kafk awaiting 5s to retry...")
                time.sleep(5)
    finally:
        spark.stop()
