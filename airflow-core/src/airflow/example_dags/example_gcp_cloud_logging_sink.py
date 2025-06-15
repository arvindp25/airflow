from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_logging_sink import (CloudLoggingCreateSinkOperator, CloudLoggingDeleteSinkOperator,CloudLoggingUpdateSinkOperator, CloudLoggingListSinksOperator )

from airflow.operators.dummy import DummyOperator

# Replace with your GCP project ID and actual destination URIs
PROJECT_ID = "	arvind-develop"
PUBSUB_DESTINATION = f"pubsub.googleapis.com/projects/{PROJECT_ID}/topics/your-topic"
BQ_DESTINATION = f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/your_dataset"

with DAG(
    dag_id="test_cloud_logging_sinks_multiple_destinations",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "gcp", "cloud-logging"],
) as dag:

    start = DummyOperator(task_id="start")

    # Create Pub/Sub Sink
    create_pubsub_sink = CloudLoggingCreateSinkOperator(
        task_id="create_pubsub_sink",
        sink_name="test-pubsub-sink",
        destination=PUBSUB_DESTINATION,
        project_id=PROJECT_ID,
    )

    # Create BigQuery Sink
    create_bq_sink = CloudLoggingCreateSinkOperator(
        task_id="create_bq_sink",
        sink_name="test-bq-sink",
        destination=BQ_DESTINATION,
        project_id=PROJECT_ID,
    )

    # Delete Pub/Sub Sink
    delete_pubsub_sink = CloudLoggingDeleteSinkOperator(
        task_id="delete_pubsub_sink",
        sink_name="test-pubsub-sink",
        project_id=PROJECT_ID,
    )

    # Delete BigQuery Sink
    delete_bq_sink = CloudLoggingDeleteSinkOperator(
        task_id="delete_bq_sink",
        sink_name="test-bq-sink",
        project_id=PROJECT_ID,
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> [create_pubsub_sink, create_bq_sink]
        >> [delete_pubsub_sink, delete_bq_sink]
        >> end
    )
