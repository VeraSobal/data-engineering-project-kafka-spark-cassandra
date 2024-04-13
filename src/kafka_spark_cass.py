import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, expr, now
from constants import(
    KAFKA_TOPIC,
    BOOTSTRAP_SERVERS,
    CASSANDRA_TABLE,
    CASSANDRA_KEYSPACE,
    SCHEMA_ARR,
    SCHEMA_DEP
    )

CASSANDRA_HOST = sys.argv[1]

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    # creates the Spark Session
    try:
        spark_session = SparkSession\
                .builder\
                .appName("SparkStructuredStreamingToCassandra")\
                .config("spark.cassandra.connection.host", CASSANDRA_HOST)\
                .config("spark.cassandra.connection.port", "9042")\
                .config("spark.cassandra.auth.username", "cassandra")\
                .config("spark.cassandra.auth.password", "cassandra")\
                .config("spark.sql.streaming.checkpointLocation", "spark-data")\
                .getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")
        spark_session = ""
    return spark_session


def get_from_kafka(spark_session):
    # reads the streaming data and creates the initial dataframe
    try:
        # gets the streaming data from topic
        df = spark_session\
              .readStream\
              .format("kafka")\
              .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
              .option("subscribe", KAFKA_TOPIC)\
              .option("startingOffsets", "latest")\
              .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
    return df


def transform(df):
    # modifies the initial dataframe, joins arrival and depertures and creates the final dataframe
    logging.info("Data is being transformed")
    df_arr = df.selectExpr("CAST(value AS STRING)")\
                .select(from_json(col("value"), SCHEMA_ARR).alias("data"))\
                .select("data.*")\
                .withColumn("a", explode(col("arrivals.arrival")))\
                .selectExpr("stationinfo.locationx locationx",
                            "stationinfo.locationy locationy",
                            "stationinfo.id id",
                            "stationinfo.name name",
                            "to_timestamp(cast(timestamp as int)) timestamp",
                            "cast (a.delay as int) delayarr",
                            "a.stationinfo.locationx locationxarr",
                            "a.stationinfo.locationy locationyarr",
                            "a.stationinfo.id idarr",
                            "a.stationinfo.name namearr",
                            "from_unixtime(cast (a.time as int)) timearr",
                            "a.platform platformarr",
                            "a.vehicle vehiclearr")\
                .withColumn("loaded_at_arr", now())\
                .withWatermark("timestamp", "1 minutes")
    df_dep = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), SCHEMA_DEP).alias("data"))\
        .select("data.*")\
        .withColumn("a", explode(col("departures.departure")))\
        .selectExpr("stationinfo.locationx station_locationx",
                    "stationinfo.locationy station_locationy",
                    "stationinfo.id station_id",
                    "stationinfo.name station_name",
                    "to_timestamp(cast(timestamp as int)) timestamp_d",
                    "cast (a.delay as int) delaydep",
                    "a.stationinfo.locationx locationxdep",
                    "a.stationinfo.locationy locationydep",
                    "a.stationinfo.id iddep",
                    "a.stationinfo.name namedep",
                    "from_unixtime(cast (a.time as int)) timedep",
                    "a.platform platformdep",
                    "a.vehicle vehicledep")\
                .withColumn("loaded_at_dep", now())\
                .withWatermark("timestamp_d", "1 minutes")
    df_final = df_dep.join(df_arr, expr("""
                                        (id = station_id AND (vehiclearr = vehicledep AND platformarr=platformdep)) AND
                                        date_trunc('minute', timestamp)=date_trunc('minute', timestamp_d) AND
                                        timestamp >= timestamp_d - interval 1 minutes AND timestamp <= timestamp_d + interval 1 minutes
                                        """), "fullOuter")\
        .withColumn("loaded_at", now())\
        .withColumn("timestamp", expr("if(timestamp is null, timestamp_d, timestamp)"))\
        .withColumn("platform", expr("if(platformarr is null, platformdep, platformarr)"))\
        .withColumn("vehicle", expr("if(vehiclearr is null, vehicledep, vehiclearr)"))\
        .withColumn("loaded_at_df", expr("if(loaded_at_arr is null, loaded_at_dep, loaded_at_arr)"))\
        .withColumn("id", expr("if(id is null, station_id, id)"))\
        .withColumn("name", expr("if(name is null, station_name, name)"))\
        .withColumn("locationx", expr("if(locationx is null, station_locationx, locationx)"))\
        .withColumn("locationy", expr("if(locationy is null, station_locationy, locationy)"))\
        .drop("station_id", "station_name", "timestamp_d",
              "station_locationx", "station_locationy",
              "vehicledep", "vehiclearr",
              "loaded_at_arr", "loaded_at_dep",
              "platformarr", "platformdep")
    return df_final


def stream_to_cass(df):
    """
    Starts the streaming to table spark_streaming.random_names in cassandra
    """
    logging.info("Streaming is being started!!!")
    df_cass = df.writeStream\
        .format("org.apache.spark.sql.cassandra")\
        .outputMode("append")\
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)\
        .start()

    return df_cass.awaitTermination()


def kafka_cass_stream():
    spark_session = create_spark_session()
    df_initial = get_from_kafka(spark_session)
    df_final = transform(df_initial)
    stream_to_cass(df_final)


if __name__ == '__main__':
    kafka_cass_stream()
