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
"""This module contains a Google Cloud Transfer sensor."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    COUNTERS,
    METADATA,
    NAME,
    CloudDataTransferServiceHook,
)
from airflow.providers.google.cloud.links.cloud_storage_transfer import CloudStorageTransferJobLink
from airflow.providers.google.cloud.triggers.cloud_storage_transfer_service import (
    CloudStorageTransferServiceCheckJobStatusTrigger,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseSensorOperator
else:
    from airflow.sensors.base import BaseSensorOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudDataTransferServiceJobStatusSensor(BaseSensorOperator):
    """
    Waits for at least one operation belonging to the job to have the expected status.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataTransferServiceJobStatusSensor`

    :param job_name: The name of the transfer job
    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
    :param project_id: (Optional) the ID of the project that owns the Transfer
        Job. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run sensor in deferrable mode
    """

    # [START gcp_transfer_job_sensor_template_fields]
    template_fields: Sequence[str] = (
        "job_name",
        "impersonation_chain",
    )
    # [END gcp_transfer_job_sensor_template_fields]
    operator_extra_links = (CloudStorageTransferJobLink(),)

    def __init__(
        self,
        *,
        job_name: str,
        expected_statuses: set[str] | str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, str) else expected_statuses
        )
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        ti = context["ti"]
        hook = CloudDataTransferServiceHook(
            gcp_conn_id=self.gcp_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operations = hook.list_transfer_operations(
            request_filter={"project_id": self.project_id or hook.project_id, "job_names": [self.job_name]}
        )

        for operation in operations:
            self.log.info("Progress for operation %s: %s", operation[NAME], operation[METADATA][COUNTERS])

        check = CloudDataTransferServiceHook.operations_contain_expected_statuses(
            operations=operations, expected_statuses=self.expected_statuses
        )
        if check:
            ti.xcom_push(key="sensed_operations", value=operations)

        project_id = self.project_id or hook.project_id
        if project_id:
            CloudStorageTransferJobLink.persist(
                context=context,
                project_id=project_id,
                job_name=self.job_name,
            )

        return check

    def execute(self, context: Context) -> None:
        """Run on the worker and defer using the triggers if deferrable is set to True."""
        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context=context):
            self.defer(
                timeout=self.execution_timeout,
                trigger=CloudStorageTransferServiceCheckJobStatusTrigger(
                    job_name=self.job_name,
                    expected_statuses=self.expected_statuses,
                    project_id=self.project_id,
                    poke_interval=self.poke_interval,
                    gcp_conn_id=self.gcp_cloud_conn_id,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        ti = context["ti"]
        ti.xcom_push(key="sensed_operations", value=event["operations"])
