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
"""This module contains Google Stackdriver links."""

from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

STACKDRIVER_BASE_LINK = "/monitoring/alerting"
STACKDRIVER_NOTIFICATIONS_LINK = STACKDRIVER_BASE_LINK + "/notifications?project={project_id}"
STACKDRIVER_POLICIES_LINK = STACKDRIVER_BASE_LINK + "/policies?project={project_id}"


class StackdriverNotificationsLink(BaseGoogleLink):
    """Helper class for constructing Stackdriver Notifications Link."""

    name = "Cloud Monitoring Notifications"
    key = "stackdriver_notifications"
    format_str = STACKDRIVER_NOTIFICATIONS_LINK


class StackdriverPoliciesLink(BaseGoogleLink):
    """Helper class for constructing Stackdriver Policies Link."""

    name = "Cloud Monitoring Policies"
    key = "stackdriver_policies"
    format_str = STACKDRIVER_POLICIES_LINK
