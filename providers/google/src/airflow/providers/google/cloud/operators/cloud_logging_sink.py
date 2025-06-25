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

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import google.cloud.exceptions
from google.api_core.exceptions import AlreadyExists
from google.cloud import logging_v2

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_logging import CloudLoggingHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


def _handle_excluison_filter(exclusion_filter):
    exclusion_filter_config = []
    if isinstance(exclusion_filter, dict):
        exclusion_filter_config.append(logging_v2.types.LogExclusion(**exclusion_filter))
    elif isinstance(exclusion_filter, list):
        for f in exclusion_filter:
            if isinstance(f, dict):
                exclusion_filter_config.append(logging_v2.types.LogExclusion(**f))
            else:
                exclusion_filter_config.append(f)
    return exclusion_filter_config


class CloudLoggingCreateSinkOperator(GoogleCloudBaseOperator):
    """
    Creates a Cloud Logging export sink in a GCP project.

    This operator creates a sink that exports log entries from Cloud Logging
    to destinations like Cloud Storage, BigQuery, or Pub/Sub.

    :param project_id: Required. The ID of the Google Cloud project.
    :param sink_config: Required. Full sink configuration dict as required by the API.
        Refer: https://cloud.google.com/logging/docs/reference/v2/rest/v2/projects.sinks
    :param unique_writer_identity: If True, creates a unique service account for the sink.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "sink_config",
        "gcp_conn_id",
        "impersonation_chain",
        "unique_writer_identity"
    )

    def __init__(
        self,
        project_id: str,
        sink_config: dict,
        unique_writer_identity: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.sink_config = sink_config
        self.unique_writer_identity = unique_writer_identity
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self):
        """Validate required inputs."""
        if not self.project_id or not self.sink_config:
            raise AirflowException("Both 'project_id' and 'sink_config' must be provided.")

        if not isinstance(self.sink_config, dict):
            raise AirflowException("`sink_config` must be a dictionary.")

        if not self.sink_config.get("name") or not self.sink_config.get("destination"):
            raise AirflowException("`sink_config` must include non-empty 'name' and 'destination'.")


    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        self._validate_inputs()
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            self.log.info("Creating log sink '%s' in project '%s'", self.sink_config["name"], self.project_id)
            self.log.info("Destination: %s", self.sink_config["destination"])
            if "filter" in self.sink_config:
                self.log.info("Filter: %s", self.sink_config["filter"])

            response = hook.create_sink(sink =logging_v2.types.LogSink(**self.sink_config),unique_writer_identity=self.unique_writer_identity, project_id = self.project_id)

            self.log.info("Log sink created successfully: %s", response.name)

            if self.unique_writer_identity and hasattr(response, "writer_identity"):
                self.log.info("Writer identity: %s", response.writer_identity)
                self.log.info("Remember to grant appropriate permissions to the writer identity")

            return logging_v2.types.LogSink.to_dict(response)

        except AlreadyExists:
            self.log.info(
                "Already existed log sink, sink_name=%s, project_id=%s",
                self.sink_config["name"],
                self.project_id,
            )
            existing_sink = hook.get_sink(sink_name= self.sink_config["name"], project_id = self.project_id)
            return logging_v2.types.LogSink.to_dict(existing_sink)

        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingDeleteSinkOperator(GoogleCloudBaseOperator):
    """
    Deletes a Cloud Logging export sink from a GCP project.

    :param sink_name: Required. Name of the sink to delete.
    :param project_id: Required. The ID of the Google Cloud project.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("sink_name", "project_id", "gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        sink_name: str,
        project_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sink_name = sink_name
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self):
        """Validate required inputs."""
        missing_fields = []
        for field_name in ["sink_name", "project_id"]:
            if not getattr(self, field_name):
                missing_fields.append(field_name)

        if missing_fields:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters must be passed as "
                "keyword parameters or as extra fields in Airflow connection definition."
            )

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        self._validate_inputs()
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        # client = hook.get_conn()
        # sink_path = f"projects/{self.project_id}/sinks/{self.sink_name}"

        try:
            sink_to_delete = hook.get_sink(sink_name =self.sink_name,project_id=self.project_id)

            self.log.info("Deleting log sink '%s' from project '%s'", self.sink_name, self.project_id)
            hook.delete_sink(sink_name =self.sink_name,project_id=self.project_id)
            self.log.info("Log sink '%s' deleted successfully", self.sink_name)

            return logging_v2.types.LogSink.to_dict(sink_to_delete)

        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingUpdateSinkOperator(GoogleCloudBaseOperator):
    """
    Updates an existing Cloud Logging export sink.

    :param sink_name: Required. Name of the sink to update.
    :param update_mask: Required.
    :param project_id: Required. The ID of the Google Cloud project.
    :param unique_writer_identity: Default True, updates the writer identity.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "sink_name",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,        
        sink_name: str,
        sink_config: dict,
        update_mask:list,
        unique_writer_identity: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.sink_name = sink_name
        self.sink_config = sink_config
        self.update_mask = update_mask
        self.unique_writer_identity = unique_writer_identity
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self):
        """Validate required inputs."""
        missing_fields = []
        for field_name in ["sink_name", "project_id", "update_mask", "sink_config"]:
            if not getattr(self, field_name):
                missing_fields.append(field_name)

        if missing_fields:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters must be passed as "
                "keyword parameters or as extra fields in Airflow connection definition."
            )

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        self._validate_inputs()
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            current_sink = hook.get_sink(sink_name = self.sink_name, project_id = self.project_id)

            self.log.info("Updating log sink '%s' in project '%s'", self.sink_name, self.project_id)
            self.log.info("Updating fields: %s", ", ".join(self.update_mask))

            response = hook.update_sink(sink_name= self.sink_name,sink = logging_v2.types.LogSink(**self.sink_config),unique_writer_identity=self.unique_writer_identity, update_mask=self.update_mask)
            self.log.info("Log sink updated successfully: %s", response.name)
            return logging_v2.types.LogSink.to_dict(response)

        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e


class CloudLoggingListSinksOperator(GoogleCloudBaseOperator):
    """
    Lists Cloud Logging export sinks in a GCP project.

    :param project_id: Required. The ID of the Google Cloud project.
    :param page_size: Optional maximum number of sinks to return per page.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("project_id", "gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        page_size: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.page_size = page_size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        """Validate required inputs."""
        if not self.project_id:
            raise AirflowException(
                "Required parameter 'project_id' is missing. This parameter must be passed as "
                "keyword parameter or as extra field in Airflow connection definition."
            )

        if self.page_size is not None and self.page_size < 0:
            raise AirflowException(
                "The page_size for the list sinks request should be greater or equal to zero"
            )

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """Execute the operator."""
        self._validate_inputs()
        hook = CloudLoggingHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        try:
            self.log.info("Listing log sinks in project '%s'", self.project_id)

            request = {"parent": parent}
            if self.page_size:
                request["page_size"] = str(self.page_size)

            sinks = hook.list_sinks(project_id=self.project_id)

            result = [logging_v2.types.LogSink.to_dict(sink) for sink in sinks]
            self.log.info("Found %d log sinks", len(result))

            return result

        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e
