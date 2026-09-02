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

"""Tests for info-body parsing and the typed namespace view."""

from aerospike_sdk.info_shared import parse_info_body, single_info_body
from aerospike_sdk.info_shared import InfoCommandsBase
from aerospike_sdk.info_types import (
    NamespaceDetail,
    SetDetail,
    Sindex,
    SindexDetail,
    StorageEngine,
    StorageFileDetail,
)

# Shape and spellings taken from a live ``namespace/test`` response.
SAMPLE_BODY = (
    "type=device;objects=17;replication-factor=2;strong-consistency=false;"
    "strong-consistency-allow-expunge=false;disallow-expunge=false;"
    "nsup-period=120;allow-ttl-without-nsup=false;default-ttl=0;"
    "storage-engine.stop-writes-avail-pct=5"
)


class TestParseInfoBody:

    def test_splits_pairs(self):
        pairs = parse_info_body(SAMPLE_BODY)
        assert pairs["type"] == "device"
        assert pairs["nsup-period"] == "120"
        assert len(pairs) == 10

    def test_dotted_keys_survive(self):
        # Storage-engine keys are dotted; the separator is ';', not '.'.
        pairs = parse_info_body(SAMPLE_BODY)
        assert pairs["storage-engine.stop-writes-avail-pct"] == "5"

    def test_only_first_equals_separates(self):
        # A value may itself contain '=' (base64 padding, expressions).
        pairs = parse_info_body("filter=a=b=c;n=1")
        assert pairs["filter"] == "a=b=c"
        assert pairs["n"] == "1"

    def test_fragments_without_equals_are_skipped(self):
        pairs = parse_info_body("type=device;garbage;n=1")
        assert pairs == {"type": "device", "n": "1"}

    def test_empty_body_yields_empty_mapping(self):
        assert parse_info_body("") == {}


class TestSingleInfoBody:

    def test_keyed_by_command(self):
        response = {"namespace/test": SAMPLE_BODY}
        assert single_info_body(response, "namespace/test") == SAMPLE_BODY

    def test_falls_back_to_sole_value_on_key_mismatch(self):
        response = {"namespace/TEST": SAMPLE_BODY}
        assert single_info_body(response, "namespace/test") == SAMPLE_BODY

    def test_ambiguous_response_without_match_is_none(self):
        response = {"a": "x=1", "b": "y=2"}
        assert single_info_body(response, "namespace/test") is None

    def test_empty_and_none_are_none(self):
        assert single_info_body(None, "namespace/test") is None
        assert single_info_body({}, "namespace/test") is None


class TestNamespaceDetailMapping:

    def test_is_a_dict(self):
        # Callers written against the previous raw-dict return keep working.
        detail = NamespaceDetail.from_body(SAMPLE_BODY)
        assert isinstance(detail, dict)

    def test_raw_keys_stay_addressable(self):
        detail = NamespaceDetail.from_body(SAMPLE_BODY)
        # No property exists for this key; it is still reachable.
        assert detail["replication-factor"] == "2"
        assert "objects" in detail
        assert dict(detail)["type"] == "device"

    def test_get_returns_default_for_absent_key(self):
        detail = NamespaceDetail.from_body(SAMPLE_BODY)
        assert detail.get("no-such-key", "fallback") == "fallback"

    def test_repr_summarizes_rather_than_dumps(self):
        detail = NamespaceDetail.from_body(SAMPLE_BODY)
        text = repr(detail)
        assert "NamespaceDetail(keys=10" in text
        assert "nsup_period=120" in text
        # A several-hundred-key document must not be inlined.
        assert "replication-factor" not in text


class TestNamespaceDetailProperties:

    def test_typed_reads(self):
        detail = NamespaceDetail.from_body(SAMPLE_BODY)
        assert detail.exists is True
        assert detail.strong_consistency is False
        assert detail.strong_consistency_allow_expunge is False
        assert detail.disallow_expunge is False
        assert detail.nsup_period == 120
        assert detail.allow_ttl_without_nsup is False
        assert detail.default_ttl == 0

    def test_true_values_coerce(self):
        detail = NamespaceDetail.from_body(
            "strong-consistency=true;allow-ttl-without-nsup=true;nsup-period=0"
        )
        assert detail.strong_consistency is True
        assert detail.allow_ttl_without_nsup is True
        assert detail.nsup_period == 0

    def test_absent_keys_fall_back_to_defaults(self):
        detail = NamespaceDetail.from_body("type=device")
        assert detail.strong_consistency is False
        assert detail.nsup_period == 0
        assert detail.default_ttl == 0

    def test_malformed_integer_falls_back(self):
        # A value the server should never send must not raise on access.
        assert NamespaceDetail.from_body("nsup-period=bogus").nsup_period == 0

    def test_underscored_strong_consistency_is_honored(self):
        # Server builds have reported this key both ways.
        assert NamespaceDetail.from_body("strong_consistency=true").strong_consistency
        assert not NamespaceDetail.from_body("strong_consistency=false").strong_consistency

    def test_unknown_type_is_not_exists(self):
        assert NamespaceDetail.from_body("type=unknown").exists is False


class TestNamespaceDetailFromResponse:

    def test_builds_from_command_keyed_response(self):
        detail = NamespaceDetail.from_response(
            {"namespace/test": SAMPLE_BODY}, "test"
        )
        assert detail is not None
        assert detail.nsup_period == 120

    def test_unknown_namespace_is_none_not_empty(self):
        # Absent and present-but-default are different answers.
        assert NamespaceDetail.from_response(
            {"namespace/nope": "type=unknown"}, "nope"
        ) is None

    def test_missing_response_is_none(self):
        assert NamespaceDetail.from_response(None, "test") is None
        assert NamespaceDetail.from_response({}, "test") is None


class TestSetDetail:
    """Typed view over one ``sets/<namespace>`` record."""

    BODY = (
        "ns=test:set=users:objects=11:tombstones=0:data_used_bytes=992"
        ":sindexes=1:index_populating=false:truncating=false:default-ttl=0"
        ":disable-eviction=false:stop-writes-count=0:stop-writes-size=0"
        ";ns=test:set=orders:objects=0:tombstones=2:data_used_bytes=0"
        ":sindexes=0:index_populating=true:truncating=true:default-ttl=3600"
        ":disable-eviction=true:stop-writes-count=10:stop-writes-size=2048"
    )

    def test_one_view_per_set(self):
        """The body is record-per-set, so a flat split yields one wrong element."""
        details = SetDetail.from_body(self.BODY)
        assert [d.name for d in details] == ["users", "orders"]

    def test_fields_convert_on_access(self):
        users, orders = SetDetail.from_body(self.BODY)
        assert users.objects == 11
        assert users.data_used_bytes == 992
        assert users.sindexes == 1
        assert users.truncating is False
        assert orders.tombstones == 2
        assert orders.truncating is True
        assert orders.index_populating is True
        assert orders.default_ttl == 3600
        assert orders.disable_eviction is True
        assert orders.stop_writes_count == 10
        assert orders.stop_writes_size == 2048

    def test_identity_fields_use_sdk_names(self):
        """The wire spells these ``ns`` and ``set``."""
        users = SetDetail.from_body(self.BODY)[0]
        assert users.namespace == "test"
        assert users.name == "users"
        assert users["ns"] == "test" and users["set"] == "users"

    def test_absent_fields_fall_back(self):
        detail = SetDetail({"ns": "test", "set": "sparse"})
        assert detail.objects == 0
        assert detail.truncating is False

    def test_empty_body_yields_nothing(self):
        assert SetDetail.from_body("") == []


class TestSindex:
    """Typed view over one normalized ``sindex-list`` entry."""

    ENTRY = {
        "namespace": "test", "set": "users", "bin": "age", "name": "by_age",
        "type": "numeric", "index_type": "default", "state": "RW",
    }

    def test_properties(self):
        index = Sindex(self.ENTRY)
        assert index.name == "by_age"
        assert index.namespace == "test"
        assert index.set_name == "users"
        assert index.bin_name == "age"
        assert index.index_type == "numeric"
        assert index.collection_type == "default"
        assert index.is_ready is True

    def test_not_ready_until_rw(self):
        assert Sindex({**self.ENTRY, "state": "WO"}).is_ready is False

    def test_expression_index_has_no_bin(self):
        assert Sindex({**self.ENTRY, "bin": ""}).bin_name == ""


class TestSindexDetail:
    """Typed view over a ``sindex/<ns>/<index>`` response."""

    BODY = (
        "entries=10;used_bytes=16777216;entries_per_bval=1"
        ";entries_per_rec=1;load_pct=100;load_time=0;stat_gc_recs=0"
    )

    def test_parses_the_body_not_the_envelope(self):
        """The response wraps the body under the command string.

        Returning that envelope satisfies an isinstance(dict) check while
        forcing the caller to know the command and parse the body themselves.
        """
        detail = SindexDetail.from_response(
            {"sindex/test/by_age": self.BODY}, "test", "by_age"
        )
        assert detail is not None
        assert "sindex/test/by_age" not in detail
        assert detail.entries == 10
        assert detail.used_bytes == 16777216
        assert detail.load_pct == 100
        assert detail.is_ready is True

    def test_missing_index_is_none(self):
        detail = SindexDetail.from_response(
            {"sindex/test/nope": "ERROR:201:no index"}, "test", "nope"
        )
        assert detail is None

    def test_empty_response_is_none(self):
        assert SindexDetail.from_response(None, "test", "by_age") is None

    def test_still_building(self):
        detail = SindexDetail.from_response(
            {"sindex/test/by_age": "entries=1;load_pct=42"}, "test", "by_age"
        )
        assert detail.load_pct == 42
        assert detail.is_ready is False


class TestStorageEngine:
    """The ``storage-engine`` section, lifted out of a namespace response."""

    NAMESPACE = {
        "replication-factor": "2",
        "storage-engine": "device",
        "storage-engine.compression": "zstd",
        "storage-engine.commit-to-device": "true",
        "storage-engine.defrag-lwm-pct": "50",
        "storage-engine.file[1]": "/data/b.dat",
        "storage-engine.file[0]": "/data/a.dat",
        "storage-engine.file[0].free_wblocks": "507",
    }

    def test_section_is_extracted_and_unprefixed(self):
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        assert engine.engine == "device"
        assert engine.compression == "zstd"
        assert engine.commit_to_device is True
        assert engine.defrag_lwm_pct == 50

    def test_unrelated_namespace_keys_are_excluded(self):
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        assert "replication-factor" not in engine

    def test_files_are_ordered_by_index_not_insertion(self):
        """The server indexes them; dict order is whatever the wire happened to give."""
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        assert [f.path for f in engine.files] == ["/data/a.dat", "/data/b.dat"]

    def test_counters_are_grouped_with_their_own_path(self):
        """The wire flattens these; only the index ties a counter to its file."""
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        first, second = engine.files
        assert first.path == "/data/a.dat"
        assert first.free_wblocks == 507
        # The counter belongs to file[0], so file[1] must not inherit it.
        assert second.path == "/data/b.dat"
        assert second.free_wblocks == 0

    def test_file_counters_default_when_unreported(self):
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        first = engine.files[0]
        assert first.used_bytes == 0
        assert first.writes == 0
        # Age is -1 rather than 0 when the server does not report it, since a
        # zero age is a real answer.
        assert first.age == -1

    def test_files_are_typed_views(self):
        engine = StorageEngine.from_namespace(self.NAMESPACE)
        assert all(isinstance(f, StorageFileDetail) for f in engine.files)

    def test_memory_engine(self):
        engine = StorageEngine.from_namespace({"storage-engine": "memory"})
        assert engine.is_memory is True
        assert engine.is_device is False
        assert engine.files == []
        assert engine.compression == "none"

    def test_reachable_from_namespace_detail(self):
        detail = NamespaceDetail(self.NAMESPACE)
        assert detail.storage_engine.engine == "device"


class TestPerNodeViews:
    """Per-node builders keep each node's own answer instead of merging."""

    def test_set_details_keep_diverging_counters(self):
        """The merged view picks one node; these are the numbers it discards."""
        responses = {
            "nodeA": {"sets/test": "ns=test:set=users:objects=254"},
            "nodeB": {"sets/test": "ns=test:set=users:objects=221"},
        }
        per_node = InfoCommandsBase._per_node_set_details(responses)
        assert {n: v[0].objects for n, v in per_node.items()} == {
            "nodeA": 254, "nodeB": 221,
        }

    def test_merge_keys_by_namespace_and_name(self):
        """Set names are unique only within a namespace.

        The unfiltered ``sets`` command spans every namespace, so keying the
        merge by name alone would silently drop a same-named set from another
        namespace -- and the survivor would look like a complete answer.
        """
        responses = {
            "nodeA": {"sets": "ns=alpha:set=users:objects=1;ns=beta:set=users:objects=2"},
        }
        merged = InfoCommandsBase._merge_set_details(responses)
        assert [(d.namespace, d.name, d.objects) for d in merged] == [
            ("alpha", "users", 1),
            ("beta", "users", 2),
        ]

    def test_namespace_details_omit_nodes_without_the_namespace(self):
        """Absent and present-but-default are different answers."""
        responses = {
            "nodeA": {"namespace/test": "type=device;strong-consistency=true"},
            "nodeB": {"namespace/test": "type=unknown"},
        }
        per_node = InfoCommandsBase._per_node_namespace_details(responses, "test")
        assert list(per_node) == ["nodeA"]
        assert per_node["nodeA"].strong_consistency is True

    def test_sindex_details_omit_nodes_without_the_index(self):
        responses = {
            "nodeA": {"sindex/test/by_age": "entries=10;load_pct=100"},
            "nodeB": {"sindex/test/by_age": "ERROR:201:no index"},
        }
        per_node = InfoCommandsBase._per_node_sindex_details(responses, "test", "by_age")
        assert list(per_node) == ["nodeA"]
        assert per_node["nodeA"].entries == 10

    def test_sindexes_keep_per_node_state(self):
        """A node mid-rebuild reports a different state than its peers."""
        entry = "ns=test:indexname=by_age:set=users:bin=age:type=numeric:indextype=default"
        responses = {
            "nodeA": {"sindex-list": f"{entry}:state=RW"},
            "nodeB": {"sindex-list": f"{entry}:state=WO"},
        }
        per_node = InfoCommandsBase._per_node_sindexes(responses)
        assert per_node["nodeA"][0].is_ready is True
        assert per_node["nodeB"][0].is_ready is False
