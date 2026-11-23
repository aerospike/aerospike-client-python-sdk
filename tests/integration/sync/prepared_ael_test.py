# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""PreparedAel through the synchronous builders."""

import pytest

from aerospike_sdk import DataSet, PreparedAel
from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_server_compiled_ael

_KEY = DataSet.of(general_namespace(), "prepared_ael_sync").id("bob")


@pytest.fixture(scope="module")
def session(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        session = c.create_session()
        session.replace(_KEY).put({"name": "o'brien", "age": 30}).execute()
        yield session
        session.delete(_KEY).execute()


@requires_server_compiled_ael
def test_where_and_select_from_bind_a_template(session):
    by_name = PreparedAel("$.name == ?name")
    assert len(list(session.query(_KEY).where(by_name, {"name": "o'brien"}).execute())) == 1
    hostile = {"name": "' or true or $.name == '"}
    assert len(list(session.query(_KEY).where(by_name, hostile).execute())) == 0
    add_years = PreparedAel("$.age + ?years")
    stream = session.query(_KEY).bin("next").select_from(add_years, {"years": 5}).execute()
    assert stream.first_or_raise().record_or_raise().bins["next"] == 35
