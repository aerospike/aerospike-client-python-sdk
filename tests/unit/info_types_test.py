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
from aerospike_sdk.info_types import NamespaceDetail

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
