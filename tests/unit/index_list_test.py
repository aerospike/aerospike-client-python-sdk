# Copyright 2025-2026 Aerospike, Inc.
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

"""Unit tests for ``sindex-list`` parsing."""

from aerospike_sdk.index_list import parse_index_list


class TestParseIndexList:
    def test_single_index(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:"
                    "indextype=default:context=null:state=RW"
                ),
            },
        }
        entries = parse_index_list(raw)
        assert len(entries) == 1
        e = entries[0]
        assert e == {
            "namespace": "test",
            "set": "users",
            "bin": "age",
            "name": "age_idx",
            "type": "numeric",
            "index_type": "default",
            "context": "null",
            "state": "RW",
        }

    def test_multiple_indexes_semicolon_separated(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:"
                    "indextype=default:state=RW;"
                    "ns=test:indexname=name_idx:set=users:bin=name:type=string:"
                    "indextype=default:state=RW"
                ),
            },
        }
        entries = parse_index_list(raw)
        assert len(entries) == 2
        assert {e["name"] for e in entries} == {"age_idx", "name_idx"}

    def test_deduplicates_across_nodes(self):
        entry = "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW"
        raw = {
            "node1": {"sindex-list": entry},
            "node2": {"sindex-list": entry},
        }
        assert len(parse_index_list(raw)) == 1

    def test_empty_response(self):
        raw = {"node1": {"sindex-list": ""}}
        assert parse_index_list(raw) == []

    def test_entry_missing_indexname_or_namespace_skipped(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "indexname=orphan;"
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW"
                ),
            },
        }
        entries = parse_index_list(raw)
        assert len(entries) == 1
        assert entries[0]["name"] == "age_idx"

    def test_missing_bin_kept_as_empty_string(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=incomplete;"
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW"
                ),
            },
        }
        entries = parse_index_list(raw)
        assert len(entries) == 2
        incomplete = next(e for e in entries if e["name"] == "incomplete")
        assert incomplete["bin"] == ""

    def test_namespace_filter(self):
        raw = {
            "node1": {
                "sindex-list": (
                    "ns=test:indexname=age_idx:set=users:bin=age:type=numeric:state=RW;"
                    "ns=other:indexname=other_idx:set=s:bin=x:type=numeric:state=RW"
                ),
            },
        }
        entries = parse_index_list(raw, namespace="test")
        assert len(entries) == 1
        assert entries[0]["name"] == "age_idx"
