import pytest
from aerospike_sdk import DataSet, SyncClient
from aerospike_sdk import exceptions
from aerospike_sdk import Exp, parse_ael


NS = "test"
SET = KEY = "ael_path_read"
DS = DataSet.of(NS, SET)

BIN_MAP = "m"
BIN_LIST = "l"
BIN_INT = "num"
BIN_INT_MAP = "im"
BIN_BLOB_MAP = "bm"

MAP = {
    "alpha": 10,
    "beta": 20,
    "gamma": 30
}
INT_KEY_MAP = {
    1: "one",
    2: "two",
    10: "ten"
}
BLOB_MAP = {
    b'\x42': "blob-val"
}
LIST = [100, 200, 300, 400, 500]
INT = 42


@pytest.fixture
def client(aerospike_host, client_policy, server_version_sync):
    if server_version_sync[:3] < (8, 1, 3):
        pytest.skip(f"This server version {server_version_sync} does not support AEL")

    k = DS.id(KEY)

    with SyncClient(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        # TODO: missing server version check. There is a async fixture for this in conftest.py
        session.upsert(k).put(
            {
                BIN_MAP: MAP,
                BIN_LIST: LIST,
                BIN_INT: INT,
                BIN_INT_MAP: INT_KEY_MAP,
                BIN_BLOB_MAP: BLOB_MAP
            }
        ).execute()

        yield c

def select_value(client: SyncClient, ael: str):
    session = client.create_session()
    rs = session.query(DS).bin("implicit").select_from(ael).execute()
    return rs.first_or_raise()


class TestAELPathRead:
    def test_implicit_map_key_read(self, client):
        # Single-select implicit get needs a resolved leaf type; bare $.m:MAP.alpha
        # leaves the value AUTO and fails server AEL compile (parameter error).
        assert MAP["alpha"] == select_value(client, "$." + BIN_MAP + ":MAP.alpha:INT")

    def test_implicit_list_index_read(self, client):
        assert LIST[1] == select_value(client, "$." + BIN_LIST + ":LIST.[1]:INT")

    def test_implicit_multi_select_values_differ_from_get_keys(self, client):
        map_values = select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}")
        assert sorted(list(MAP.values())) == sorted(map_values)

        map_keys = select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}.getKeys()")
        assert set(MAP.keys()) == set(map_keys)

    def test_get_key_values_returns_flat_key_value_list(self, client):
        flat = select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta}.getKeyValues()")

        assert len(flat) == 4
        assert flat[0] == "alpha"
        assert flat[1] == MAP["alpha"]
        assert flat[2] == "beta"
        assert flat[3] == MAP["beta"]

    def test_get_tree_preserves_map_structure(self, client):
        tree = select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta}.getTree()")

        assert isinstance(tree, dict)
        assert tree["alpha"] == MAP["alpha"]
        assert tree["beta"] == MAP["beta"]
        assert len(tree) == 2

    def test_get_tree_on_full_map_key_list(self, client):
        assert len(MAP) == select_value(client, "$." + BIN_MAP + ":MAP.count()")

    def test_count_on_whole_list_is_element_size(self, client):
        assert len(LIST) == select_value(client, "$." + BIN_LIST + ":LIST.count()")

    def test_count_on_multi_select_map_key_list_is_match_count(self, client):
        assert len(MAP) == select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}.count()")

    def test_count_on_multi_select_list_range_is_match_count(self, client):
        assert len(LIST[0:3]) == select_value(client, "." + BIN_LIST + ":LIST.[0:3].count()")

    def test_count_on_single_map_key_is_not_applicable(self, client):
        # TODO: couldn't find an exception for server code OP_NOT_APPLICABLE
        with pytest.raises(exceptions.AerospikeError):
            select_value(client, "$." + BIN_MAP + ":MAP.alpha:INT.count()")

    def test_typed_scalar_bin_read(self, client):
        assert INT == select_value(client, "$." + BIN_INT + ":INT")

    def test_typed_map_value_read(self, client):
        assert MAP["beta"] == select_value(client, "$." + BIN_MAP + ":MAP.beta:INT")

    def test_typed_path_used_in_filter(self, client):
        session = client.create_session()
        rs = session.query(DS) \
            .bin("hit") \
            .select_from("$." + BIN_MAP + ":MAP.beta:INT") \
            .where("$." + BIN_MAP + ":MAP.beta:INT == 20") \
            .execute()
        rec = rs.first_or_raise()

        assert MAP["beta"] == rec.record.bins["hit"]

class TestWildcard:
    def test_list_wildcard_filter_on_typed_value(self, client):
        expected_length = len([x for x in LIST if x > 250])
        assert expected_length == select_value(client, "$." + BIN_LIST + ":LIST.*[?(@:INT > 250)].count()")

    def test_list_wildcard_filter_on_value_no_match(self, client):
        assert 0 == select_value(client, ":LIST.*[?(@:INT > 900)].count()")

    def test_map_wildcard_filter_on_value(self, client):
        expected_length = len([value for value in MAP.values() if value > 15])
        assert expected_length == select_value(client, "$." + BIN_MAP + ":MAP.*[?(@ > 15)].count()")

    def test_map_wildcard_filter_on_key(self, client):
        assert 1 == select_value(client, "$." + BIN_MAP + ":MAP.*[?(@key == 'beta')].count()")

    def test_list_wildcard_filter_on_index(self, client):
        assert len(LIST[3:]) == select_value(client, "$." + BIN_LIST + ":LIST.*[?(@index >= 3)].count()")

    def test_wildcard_filter_selects_matching_values(self, client):
        values = select_value(client, "$." + BIN_MAP + ":MAP.*[?(@ > 15)]")
        expected_values = [value for value in MAP.values() if value > 15]
        assert sorted(expected_values) == sorted(list(values))

    def matches_where(self, client: SyncClient, ael: str):
        session = client.create_session()
        # TODO: where() doesn't return a SyncQueryBuilder
        rs = session.query(DS).where(ael).execute()
        rs.first_or_raise()

    def test_where_matches_when_wildcard_predicate_true(self, client):
        self.matches_where(client, "$." + BIN_LIST + ":LIST.*[?(@:INT > 400)].count() >= 1")

    def test_where_matches_when_wildcard_predicate_false(self, client):
        with pytest.raises(StopIteration):
            self.matches_where(client, "$." + BIN_LIST + ":LIST.*[?(@:INT > 500)].count() >= 1")

    def test_typed_value_pin_narrows_list_wildcard(self, client):
        assert 1 == select_value(client, "$." + BIN_LIST + ":LIST.*[?(@:INT == 200)].count()")

    def test_typed_string_key_pin_on_map_wildcard(self, client):
        assert 1 == select_value(client, "$." + BIN_MAP + ":MAP.*[?(@key:STRING == 'gamma')].count()")

    def test_typed_int_key_pin_on_int_map_wildcard(self, client):
        assert 1 == select_value(client, "$." + BIN_INT_MAP + ":MAP.*[?(@key:INT == 2)].count()")

    def test_typed_blob_key_pin_on_blob_map_wildcard(self, client):
        assert 1 == select_value(client, "$." + BIN_BLOB_MAP + ":MAP.*[?(@key:BLOB == X'42')].count()")

class TestAndFilter:
    def test_and_filter_after_index_range(self, client):
        assert 2 == select_value(client, "$." + BIN_LIST + ":LIST.[1:4]&[?(@:INT >= 300)].count()")

    def test_and_filter_after_key_list(self, client):
        assert 2 == select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}&[?(@ > 15)].count()")

    def test_and_filter_after_loop_var_key_list(self, client):
        assert 2 == select_value(client, "$." + BIN_MAP + ":MAP.{@\"alpha\", \"beta\", \"gamma\"}&[?(@ > 15)].count()")

    def test_and_filter_after_value_interval(self, client):
        assert 1 == select_value(client, "$." + BIN_MAP + ":MAP.{=10:31}&[?(@key == 'gamma')].count()")

    def test_and_filter_narrows_key_list_selection(self, client):
        keys = select_value(client, "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}&[?(@ > 15)].getKeys()")
        assert sorted(keys) == sorted(["beta", "gamma"])
