from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.cloud_logging_sink import (CloudLoggingCreateSinkOperator, CloudLoggingDeleteSinkOperator,CloudLoggingUpdateSinkOperator, CloudLoggingListSinksOperator )

# Constants
PROJECT_ID = "	arvind-develop"
SINK_NAME = "my-airflow-test-sink"
DESTINATION = "storage.googleapis.com/log_sink"
UPDATED_DESTINATION = "storage.googleapis.com/log_sinl"
CONN_ID = "google_cloud_default"

with DAG(
    dag_id="cloud_logging_sink_workflow",
    description="Manage a GCP Cloud Logging Sink using custom Airflow operators",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gcp", "cloud-logging", "custom"],
) as dag:

    create_sink = CloudLoggingCreateSinkOperator(
        task_id="create_sink",
        project_id=PROJECT_ID,
        sink_name=SINK_NAME,
        destination=DESTINATION,
        gcp_conn_id=CONN_ID,
    )

    update_sink = CloudLoggingUpdateSinkOperator(
        task_id="update_sink",
        project_id=PROJECT_ID,
        sink_name=SINK_NAME,
        destination=UPDATED_DESTINATION,
        gcp_conn_id=CONN_ID,
    )

    list_sinks = CloudLoggingListSinksOperator(
        task_id="list_sinks",
        project_id=PROJECT_ID,
        gcp_conn_id=CONN_ID,
    )

    delete_sink = CloudLoggingDeleteSinkOperator(
        task_id="delete_sink",
        project_id=PROJECT_ID,
        sink_name=SINK_NAME,
        gcp_conn_id=CONN_ID,
    )

    create_sink >> update_sink >> list_sinks >> delete_sink
