from pyspark.sql.types import StructType, StructField, StringType, ArrayType
# default list of stations
STATION_LIST_DEFAULT = ['BE.NMBS.008811304',
                        'BE.NMBS.008811916',
                        'BE.NMBS.008812005',
                        'BE.NMBS.008813003',
                        'BE.NMBS.008813037',
                        'BE.NMBS.008813045',
                        'BE.NMBS.008814001',
                        'BE.NMBS.008815040',
                        'BE.NMBS.008819406']
# API params
ARRDEPPAR = ["arrival", "departure"]
LOCAL_TZ = "Europe/Minsk"
URL_API_LIVEBOARD = "?id={}&arrdep={}&format=json&alerts=true"
# KAFKA params
BOOTSTRAP_SERVERS = "kafka1:19092,kafka2:19093,kafka3:19094"
KAFKA_TOPIC = "liveboard"
# Cassandra params
CASSANDRA_TABLE = "liveboard"
CASSANDRA_KEYSPACE = "railway"
CASSANDRA_HOST_DEFAULT = "158.160.70.157"


def schema(arr_dep):
    # creates schema for API liveboard json
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("station", StringType(), True),
        StructField("stationinfo", StructType([
            StructField("locationX", StringType(), True),
            StructField("locationY", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            ]), True),
        StructField(arr_dep+"s", StructType([
            StructField(arr_dep, ArrayType(
                StructType([
                    StructField("id", StringType(), True),
                    StructField("delay", StringType(), True),
                    StructField("station", StringType(), True),
                    StructField("stationinfo", StructType([
                        StructField("locationX", StringType(), True),
                        StructField("locationY", StringType(), True),
                        StructField("id", StringType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                    StructField("time", StringType(), True),
                    StructField("vehicle", StringType(), True),
                    StructField("vehicleinfo", StructType([
                        StructField("name", StringType(), True),
                        StructField("shortname", StringType(), True),
                        StructField("number", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("locationx", StringType(), True),
                        StructField("locationy", StringType(), True),
                        StructField("@id", StringType(), True),
                    ]), True),
                    StructField("platform", StringType(), True),
                    StructField("platforminfo", StructType([
                        StructField("name", StringType(), True),
                        StructField("normal", StringType(), True),
                    ]), True),
                    StructField("canceled", StringType(), True),
                    StructField("left", StringType(), True),
                    StructField("isExtra", StringType(), True),
                    StructField("departureConnection", StringType(), True),
                ])
            ), True),
        ]), True),
    ])

 # API json schema for spark dataframe
SCHEMA_ARR = schema("arrival")
SCHEMA_DEP = schema("departure")
