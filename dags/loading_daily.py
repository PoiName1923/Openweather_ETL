from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, desc
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
import time

now_vietnam = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
current_date = now_vietnam.strftime("%Y-%m-%d")

jars = [
    "/opt/airflow/jars/postgresql-42.7.5.jar",
    "/opt/airflow/jars/commons-pool2-2.12.0.jar",
]
def initial_spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Loading Process") \
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

def data_collected(spark,logger):
    logger.info("Identify and aggregate data...")
    df = spark.read.parquet(f"hdfs://namenode:9000/data/weather/date={current_date}").orderBy(desc("timestamp"))
    # Dữ liệu định danh cho City
    cities_df = df.select(
        "city_id",
        "city_name",
        "latitude",
        "longitude"
    ).distinct()

    # Dữ liệu thời tiết của City
    weather_df = df.select(
        "city_id",
        "timestamp",
        "temperature_celsius",
        "feels_like_celsius",
        "temp_min_celsius",
        "temp_max_celsius",
        "pressure_hpa",
        "humidity_percent",
        "wind_speed_mps",
        "wind_direction_deg",
        "wind_gust_mps",
        "rain_1h_mm",
        "cloud_coverage_percent",
        "weather_condition",
        "weather_description",
        "sunrise_time",
        "sunset_time"
    )
    # Bảng tổng hợp
    daily_agg = weather_df.groupBy("city_id").agg(
        # Thời gian cập nhật
        F.current_date().alias("aggregate_date"),
        # Nhiệt độ
        F.avg("temperature_celsius").alias("avg_temperature"),
        F.max("temperature_celsius").alias("max_temperature"),
        F.min("temperature_celsius").alias("min_temperature"),
        # Nhiệt độ cảm nhận
        F.avg("feels_like_celsius").alias("avg_feels_like"),
        F.max("feels_like_celsius").alias("max_feels_like"),
        F.min("feels_like_celsius").alias("min_feels_like"),
        # Áp suất
        F.avg("pressure_hpa").alias("avg_pressure"),
        F.max("pressure_hpa").alias("max_pressure"),
        F.min("pressure_hpa").alias("min_pressure"),
        # Độ ẩm
        F.avg("humidity_percent").alias("avg_humidity"),
        F.max("humidity_percent").alias("max_humidity"),
        F.min("humidity_percent").alias("min_humidity"),
        # Gió
        F.avg("wind_speed_mps").alias("avg_wind_speed"),
        F.max("wind_speed_mps").alias("max_wind_speed"),
        F.min("wind_speed_mps").alias("min_wind_speed"),
        # Giật gió
        F.max("wind_gust_mps").alias("max_wind_gust"),
        # Lượng mưa
        F.sum("rain_1h_mm").alias("total_rainfall"),
        # Số lần đo
        F.count("*").alias("measurement_count"),
        
    )
    logger.info("Identify and aggregate Successfully")
    return cities_df, weather_df, daily_agg

def loading_to_postgres(spark, cities_df, weather_df, daily_agg):
    url = "jdbc:postgresql://postgres:5432/weather_database"
    properties = {
        "user": "ndtienpostgres",
        "password": "ndtienpostgres",
        "driver": "org.postgresql.Driver"
    }
    logger.info("Start loading to Postgres Database : weather_database")
    # đọc dữ liệu cũ 
    old_cities_df = spark.read.jdbc(url=url,table="cities",properties=properties)
    new_cities_df = cities_df.join(old_cities_df, on='city_id', how='left_anti')

    # Ghi dữ liệu lên Cities
    new_cities_df.write.jdbc(url=url, table="cities", mode="append", properties=properties)
    # Ghi dữ liệu lên weather_measurements
    weather_df.write.jdbc(url=url, table="weather_measurements", mode="append", properties=properties)
    # Ghi dữ liệu lên Cities
    daily_agg.write.jdbc(url=url, table="weather_daily_aggregates", mode="overwrite", properties=properties)
    logger.info("Loading to Postgres Database : weather_dabase successfully")

if __name__ == "__main__":
    max_retries = 20
    spark,logger = initial_spark()
    for attemp in range(max_retries):
        cities_df, weather_data_df, weather_agg_df = data_collected(spark=spark,logger=logger)
        if cities_df and weather_data_df and weather_agg_df:
            try:
                loading_to_postgres(spark=spark,cities_df=cities_df,
                                    weather_df=weather_data_df,
                                    daily_agg=weather_agg_df)
                break
            except Exception as e:
                logger.error(f"Error loading data to Postgres: {str(e)}")
                logger.info("Waiting 5s for retry...")
                time.sleep(5)
        else: 
            logger.error("Error collected data from HDFS")      
            logger.info("Waiting 5s for retry...")
            time.sleep(5)              