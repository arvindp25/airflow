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
"""This module contains Google Workflows links."""

from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

WORKFLOWS_BASE_LINK = "/workflows"
WORKFLOW_LINK = WORKFLOWS_BASE_LINK + "/workflow/{location_id}/{workflow_id}/executions?project={project_id}"
WORKFLOWS_LINK = WORKFLOWS_BASE_LINK + "?project={project_id}"
EXECUTION_LINK = (
    WORKFLOWS_BASE_LINK
    + "/workflow/{location_id}/{workflow_id}/execution/{execution_id}?project={project_id}"
)


class WorkflowsWorkflowDetailsLink(BaseGoogleLink):
    """Helper class for constructing Workflow details Link."""

    name = "Workflow details"
    key = "workflow_details"
    format_str = WORKFLOW_LINK


class WorkflowsListOfWorkflowsLink(BaseGoogleLink):
    """Helper class for constructing list of Workflows Link."""

    name = "List of workflows"
    key = "list_of_workflows"
    format_str = WORKFLOWS_LINK


class WorkflowsExecutionLink(BaseGoogleLink):
    """Helper class for constructing Workflows Execution Link."""

    name = "Workflow Execution"
    key = "workflow_execution"
    format_str = EXECUTION_LINK
