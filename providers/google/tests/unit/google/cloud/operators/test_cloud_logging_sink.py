#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module contains various unit tests for GCP Cloud Build Operators
"""

from __future__ import annotations

from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud.exceptions import GoogleCloudError, NotFound
import google.cloud.exceptions
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.logging_v2.types  import LogSink

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_logging_sink import (
    CloudLoggingCreateSinkOperator,
    CloudLoggingDeleteSinkOperator,
    CloudLoggingUpdateSinkOperator,
    CloudLoggingListSinksOperator
)


CLOUD_LOGGING_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingHook"
TASK_ID = "test-task"
SINK_NAME = "test-sink"
PROJECT_ID = "test-project"
DESTINATION = "pubsub.googleapis.com/projects/test-project/topics/test-topic"
SINK_PATH = f"projects/{PROJECT_ID}/sinks/{SINK_NAME}"
UNIQUE_WRITER_IDENTITY = True

log_sink = LogSink()
log_sink.name = SINK_NAME
log_sink.destination = DESTINATION

def _assert_common_template_fields(template_fields):
    assert "project_id" in template_fields
    assert "gcp_conn_id" in template_fields
    assert "impersonation_chain" in template_fields


# class TestCloudLoggingCreateSinkOperator:
#     def test_template_fields(self):
#         operator = CloudLoggingCreateSinkOperator(
#             task_id = TASK_ID,sink_name=SINK_NAME,destination = DESTINATION , project_id=PROJECT_ID,
#         )

#         _assert_common_template_fields(operator.template_fields)

#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create(self, hook_mock):
#         client_mock = mock.MagicMock()
#         client_mock.create_sink.return_value = log_sink 

#         # Set get_conn() to return the mocked client
#         hook_mock.return_value.get_conn.return_value = client_mock

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=DESTINATION,
#             project_id=PROJECT_ID,
#         )

#         operator.execute(context=mock.MagicMock())

#         client_mock.create_sink.assert_called_once_with(
#             request = {
#             "parent" : f"projects/{PROJECT_ID}",
#             "sink" : log_sink,
#             "unique_writer_identity": UNIQUE_WRITER_IDENTITY
#             }
#         )
#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create(self, hook_mock):
#         client_mock = mock.MagicMock()
#         client_mock.create_sink.return_value = log_sink 
#         hook_mock.return_value.get_conn.return_value = client_mock

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=DESTINATION,
#             project_id=PROJECT_ID,
#         )

#         operator.execute(context=mock.MagicMock())

#         client_mock.create_sink.assert_called_once_with(
#             request = {
#             "parent" : f"projects/{PROJECT_ID}",
#             "sink" : log_sink,
#             "unique_writer_identity": UNIQUE_WRITER_IDENTITY
#             }
#         )

#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create_sink_already_exists(self, hook_mock):
#         existing_sink = LogSink(name=SINK_NAME, destination=DESTINATION)

#         client_mock = mock.MagicMock()
#         # Simulate AlreadyExists on create_sink
#         client_mock.create_sink.side_effect = AlreadyExists("Sink already exists")
#         # Simulate existing sink returned by get_sink fallback
#         client_mock.get_sink.return_value = existing_sink
#         hook_mock.return_value.get_conn.return_value = client_mock

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=DESTINATION,
#             project_id=PROJECT_ID,
#         )

#         result = operator.execute(context=mock.MagicMock())

#         client_mock.create_sink.assert_called_once()
#         client_mock.get_sink.assert_called_once()


#         assert result == LogSink.to_dict(existing_sink)

#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create_sink_raises_error(self,hook_mock):
#         client_mock = mock.MagicMock()
#         client_mock.create_sink.side_effect = GoogleAPICallError("Failed to create sink")
#         hook_mock.return_value.get_conn.return_value = client_mock

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=DESTINATION,
#             project_id=PROJECT_ID,
#         )

#         with pytest.raises(GoogleAPICallError, match="Failed to create sink"):
#             operator.execute(context=mock.MagicMock())

#         client_mock.create_sink.assert_called_once()

#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create_with_impersonation_chain(self, hook_mock):
#         client_mock = mock.MagicMock()
#         client_mock.create_sink.return_value = log_sink

#         hook_mock.return_value.get_conn.return_value = client_mock

#         impersonation_chain = ["user1@project.iam.gserviceaccount.com"]

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=DESTINATION,
#             project_id=PROJECT_ID,
#             impersonation_chain=impersonation_chain
#         )

#         operator.execute(context=mock.MagicMock())

#         hook_mock.assert_called_once_with(
#             gcp_conn_id="google_cloud_default",
#             impersonation_chain=impersonation_chain
#         )

#     def test_create_with_empty_sink_name_raises(self):
#         with pytest.raises(AirflowException,  match=r"Required parameters are missing: \['sink_name'\]\. These parameters must be passed as keyword parameters or as extra fields in Airflow connection definition\."):
#             CloudLoggingCreateSinkOperator(
#                 task_id=TASK_ID,
#                 sink_name="",
#                 destination=DESTINATION,
#                 project_id=PROJECT_ID
#             )



#     @pytest.mark.parametrize("destination", [
#         "storage.googleapis.com/test-bucket",
#         "bigquery.googleapis.com/projects/project/datasets/dataset",
#         "pubsub.googleapis.com/projects/project/topics/topic-name",
#     ])
#     @mock.patch(CLOUD_LOGGING_HOOK_PATH)
#     def test_create_with_different_destinations(self, hook_mock, destination):
#         client_mock = mock.MagicMock()
#         client_mock.create_sink.return_value = log_sink
#         hook_mock.return_value.get_conn.return_value = client_mock

#         operator = CloudLoggingCreateSinkOperator(
#             task_id=TASK_ID,
#             sink_name=SINK_NAME,
#             destination=destination,
#             project_id=PROJECT_ID,
#         )
#         operator.execute(context=mock.MagicMock())

#         assert operator.destination == destination

#         client_mock.create_sink.assert_called_once_with(
#             request={
#                 "parent": f"projects/{PROJECT_ID}",
#                 "sink": mock.ANY,  # You can validate fields inside if needed
#                 "unique_writer_identity": UNIQUE_WRITER_IDENTITY,
#             }
#         )

class TestCloudLoggingDeleteSinkOperator:

    def test_template_fields(self):
        operator = CloudLoggingDeleteSinkOperator(
            task_id = TASK_ID,sink_name=SINK_NAME, project_id=PROJECT_ID,
        )
        assert "sink_name" in operator.template_fields
        _assert_common_template_fields(operator.template_fields)

    def test_missing_required_params(self):
            with pytest.raises(AirflowException) as excinfo:
                CloudLoggingDeleteSinkOperator(
                    task_id=TASK_ID,
                    sink_name=None,
                    project_id=None,
                )
            assert "Required parameters are missing" in str(excinfo.value)


    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_success(self, hook_mock):
        client_mock = mock.MagicMock()
        log_sink = LogSink(name=SINK_NAME, destination=DESTINATION)
        client_mock.get_sink.return_value = log_sink
        client_mock.delete_sink.return_value = None
        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        context = mock.MagicMock()
        result = operator.execute(context=context)

        client_mock.get_sink.assert_called_once()
        client_mock.delete_sink.assert_called_once()

        assert result == LogSink.to_dict(log_sink)

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_not_found(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.get_sink.side_effect = NotFound("Sink not found")

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(NotFound):
            operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once()
        client_mock.delete_sink.assert_not_called()

    @mock.patch(CLOUD_LOGGING_HOOK_PATH)
    def test_delete_sink_raises_error(self, hook_mock):
        client_mock = mock.MagicMock()
        client_mock.get_sink.side_effect = GoogleCloudError("Internal error")

        hook_mock.return_value.get_conn.return_value = client_mock

        operator = CloudLoggingDeleteSinkOperator(
            task_id=TASK_ID,
            sink_name=SINK_NAME,
            project_id=PROJECT_ID,
        )

        with pytest.raises(GoogleCloudError):
            operator.execute(context=mock.MagicMock())

        client_mock.get_sink.assert_called_once()
        client_mock.delete_sink.assert_not_called()

 