from airflow import DAG
from airflow.models import Variable, DagModel
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pendulum
from datetime import timedelta
import json
import logging
import sys

from src.constants import (
    STATION_LIST_DEFAULT,
    URL_API_LIVEBOARD,
    ARRDEPPAR,
    LOCAL_TZ,
    KAFKA_TOPIC,
    CASSANDRA_HOST_DEFAULT
)

STATION_LIST = Variable.get("station_list", default_var=STATION_LIST_DEFAULT, deserialize_json=True)
CASSANDRA_HOST = Variable.get("cassandra_host", default_var=CASSANDRA_HOST_DEFAULT, deserialize_json=True)

log = logging.getLogger("airflow.task.operators")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)

path_jars = "/home/resources/"
path_spark_app = "/home/src/"


def kwargs_kv(station_id):
    # gets data from HTTPOperator
    keys = []
    values = []
    for arr_dep in ARRDEPPAR:
        keys.append(f'kafka_data_{station_id}_{arr_dep}')
        values.append(api_kafka_dag.get_task(f'request_data_{station_id}_{arr_dep}').output)
    return {key: value for key, value in zip(keys, values)}


def produce_to_kafka(station_id, **kwargs):
    # prepares data for kafka
    data = {}
    for arr_dep in ARRDEPPAR:
        log.info(f"{station_id}_{arr_dep}")
        data[arr_dep] = kwargs.get(f"kafka_data_{station_id}_{arr_dep}")
    # to ensure that data both for arrival and departure is recieved from api
    if len(data) == len(ARRDEPPAR):
        log.info("Send data to kafka operator")
        for arr_dep in ARRDEPPAR:
            yield (arr_dep, json.dumps(data[arr_dep]))


with DAG(
        dag_id='api_kafka_dag',
        schedule='* * * * *',
        start_date=pendulum.datetime(2024, 3, 20, 12, 0, tz=LOCAL_TZ),
        catchup=False,
        max_active_tasks=2,
        max_active_runs=1,
        description='Railway Api-Kafka-Spark-Cassandra DAG',
        tags=['kafka', 'API', 'railway'],
) as api_kafka_dag:

    request_data_task = [[HttpOperator(
        http_conn_id="irail_liveboard",
        task_id=f"request_data_{station_id}_{arr_dep}",
        method="GET",
        endpoint=URL_API_LIVEBOARD.format(station_id, arr_dep),
        execution_timeout=timedelta(seconds=30),
        response_filter=lambda response: response.json(),
        do_xcom_push=True,
        dag=api_kafka_dag
    ) for arr_dep in ARRDEPPAR] for station_id in STATION_LIST]

    produce_to_kafka_task = [ProduceToTopicOperator(
        task_id=f"produce_to_kafka_{station_id}",
        kafka_config_id='kafka3',
        topic=KAFKA_TOPIC,
        producer_function=produce_to_kafka,
        producer_function_args=[station_id],
        producer_function_kwargs=kwargs_kv(station_id),
        dag=api_kafka_dag
    ) for station_id in STATION_LIST]

    for i in range(len(request_data_task)-1):
        request_data_task[i] >> produce_to_kafka_task[i]


with DAG(
        dag_id='kafka_spark_cass_dag',
        schedule_interval=None,
        catchup=False,
        max_active_tasks=1,
        max_active_runs=1,
        description='Railway Kafka-Spark-Cassandra DAG',
        tags=['spark', 'cassandra', 'railway', 'kafka'],
) as kafka_spark_cass_dag:

    spark_submit_task = SparkSubmitOperator(
        task_id="spark_submit_task",
        conn_id="spark_master",
        application=path_spark_app+"kafka_spark_cass.py",
        application_args=[CASSANDRA_HOST],
        conf={"spark.master": "spark://spark:7077"},
        packages="com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        jars=",".join(list(map(lambda x: path_jars+x, ["spark-sql-kafka-0-10_2.12-3.4.1.jar",
                                                       "kafka-clients-3.4.1.jar",
                                                       "spark-token-provider-kafka-0-10_2.12-3.4.1.jar",
                                                       "commons-pool2-2.12.0.jar",
                                                       "spark-cassandra-connector-assembly_2.12-3.4.1.jar"]))),
        dag=kafka_spark_cass_dag
        )

    spark_submit_task
