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
from typing import TYPE_CHECKING

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.version_compat import BaseOperator
from airflow.providers.presto.hooks.presto import PrestoHook

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class PrestoToMySqlOperator(BaseOperator):
    """
    Moves data from Presto to MySQL.

    Note that for now the data is loaded into memory before being pushed
    to MySQL, so this operator should be used for smallish amount of data.

    :param sql: SQL query to execute against Presto. (templated)
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    :param presto_conn_id: source presto connection
    :param mysql_preoperator: sql statement to run against mysql prior to
        import, typically use to truncate of delete in place
        of the data coming in, allowing the task to be idempotent (running
        the task twice won't double load data). (templated)
    """

    template_fields: Sequence[str] = ("sql", "mysql_table", "mysql_preoperator")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {
        "sql": "sql",
        "mysql_preoperator": "mysql",
    }
    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        sql: str,
        mysql_table: str,
        presto_conn_id: str = "presto_default",
        mysql_conn_id: str = "mysql_default",
        mysql_preoperator: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.presto_conn_id = presto_conn_id

    def execute(self, context: Context) -> None:
        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        self.log.info("Extracting data from Presto: %s", self.sql)
        results = presto.get_records(self.sql)

        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        if self.mysql_preoperator:
            self.log.info("Running MySQL preoperator")
            self.log.info(self.mysql_preoperator)
            mysql.run(self.mysql_preoperator)

        self.log.info("Inserting rows into MySQL")
        mysql.insert_rows(table=self.mysql_table, rows=results)
