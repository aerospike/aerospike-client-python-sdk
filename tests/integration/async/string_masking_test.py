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

"""End-to-end masking coverage for PSDK's ``str_*`` chainable surface.

Verifies that PSDK's ``WriteBinBuilder.str_*`` and ``QueryBinBuilder.str_*``
methods observe server-side masking rules correctly — privileged readers
see real values, unprivileged readers see masked values (redacted or
constant), and modifies are blocked for unprivileged users.

Gated on FOUR conditions:

1. ``AEROSPIKE_HOST_8_1_3`` set (string ops + masking are 8.1.3+ features)
2. Security enabled on the target cluster
3. Admin credentials supplied via ``AEROSPIKE_HOST_8_1_3_USER`` /
   ``AEROSPIKE_HOST_8_1_3_PASSWORD``
4. Server accepts ``masking;...`` info commands

PSDK exposes ``Session.info()`` so masking rules are applied via the PSDK
surface. User/role management is not on PSDK's SDK layer (by design —
expect ``asadm`` or the low-level client for that), so user setup
borrows PAC's ``new_client`` admin path. Test bodies and masking-rule
plumbing go through PSDK; only ``create_user`` / ``drop_user`` /
``grant_roles`` drop to PAC.
"""

import asyncio
import os

import pytest
import pytest_asyncio

from aerospike_async import AuthMode, ClientPolicy as _PacClientPolicy, new_client
from aerospike_async.exceptions import ResultCode, ServerError, SecurityNotEnabled

from aerospike_sdk import Client, DataSet


pytestmark = pytest.mark.asyncio(loop_scope="module")


_NAMESPACE = "test"
_SET = "tmsk_psdk"
_BIN_MASKED = "pii"
_BIN_UNMASKED = "public"
_BIN_CONSTANT = "secret"
_RECORD_KEY = "psdk_mask_record"

# PSDK-specific user names to avoid collision with PAC's parallel suite
_USER_READER = "psdk_strops_reader"
_USER_BASIC = "psdk_strops_user"
_USER_PASSWORD = "test_password_123"
_PROPAGATION_RETRIES = 10
_PROPAGATION_DELAY = 0.5


_TEST_DS = DataSet.of(_NAMESPACE, _SET)


def _services_alternate_813() -> bool:
    sa_override = os.environ.get("AEROSPIKE_HOST_8_1_3_USE_SERVICES_ALTERNATE")
    if sa_override is not None:
        return sa_override.lower() == "true"
    return os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "true").lower() == "true"


def _is_role_violation(exc) -> bool:
    """Match a ROLE_VIOLATION (code 81) heuristically across PAC's exception shapes."""
    code_repr = str(getattr(exc, "result_code", "")).lower()
    msg = str(exc).lower()
    type_name = type(exc).__name__.lower()
    needles = ("roleviolation", "role violation", "forbidden", "fail_forbidden")
    return any(n in code_repr or n in msg or n in type_name for n in needles)


# ---------------------------------------------------------------------------
# Admin setup via PAC (PSDK Client does not expose user/role/info APIs)
# ---------------------------------------------------------------------------

async def _wait_for_user(admin_pac, username, *, retries=_PROPAGATION_RETRIES):
    """Retry query_users until ``username`` is visible (SMD propagation)."""
    for _ in range(retries):
        try:
            users = await admin_pac.query_users(None)
            if any(u.user == username for u in users):
                return
        except ServerError:
            pass
        await asyncio.sleep(_PROPAGATION_DELAY)
    pytest.fail(f"User {username!r} not visible after {retries} retries")


async def _apply_masking(admin_session, *, ns, set_name, bin_name, function, value=None):
    """Apply a masking rule via PSDK ``Session.info``."""
    parts = [
        f"set={set_name}",
        f"namespace={ns}",
        f"bin={bin_name}",
        "type=string",
        f"function={function}",
    ]
    if value is not None:
        parts.append(f"value={value}")
    cmd = "masking;" + ";".join(parts)
    response = await admin_session.info(cmd)
    for node_response in response.values():
        if node_response != "ok":
            raise RuntimeError(f"masking command failed: {cmd} → {node_response}")


async def _remove_masking(admin_session, *, ns, set_name, bin_name):
    cmd = (
        f"masking;set={set_name};namespace={ns};bin={bin_name};"
        f"type=string;function=remove"
    )
    try:
        await admin_session.info(cmd)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def admin_pac(aerospike_host_8_1_3):
    """PAC admin client — used ONLY for user/role management (create_user,
    drop_user, grant_roles, query_users), which PSDK does not expose at
    the SDK layer (by design; users are expected to use ``asadm`` or the
    low-level client for that). Skips the module if security is not
    enabled or admin creds fail.
    """
    if not aerospike_host_8_1_3:
        pytest.skip("AEROSPIKE_HOST_8_1_3 unset")
    user = os.environ.get("AEROSPIKE_HOST_8_1_3_USER", "admin")
    password = os.environ.get("AEROSPIKE_HOST_8_1_3_PASSWORD", "admin")
    cp = _PacClientPolicy()
    cp.use_services_alternate = _services_alternate_813()
    cp.user = user
    cp.password = password
    try:
        client = await new_client(cp, aerospike_host_8_1_3)
    except Exception as exc:
        pytest.skip(f"Could not connect as admin to {aerospike_host_8_1_3}: {exc}")
    await asyncio.sleep(2)
    try:
        await client.query_users(None)
    except ServerError as exc:
        await client.close()
        if exc.result_code == ResultCode.SECURITY_NOT_ENABLED or isinstance(exc, SecurityNotEnabled):
            pytest.skip("Security not enabled on 8.1.3+ cluster")
        raise
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def admin_client(aerospike_host_8_1_3, admin_pac):
    """PSDK admin Client — used for masking-rule application, record put,
    and any test assertion that needs admin privileges. Depends on
    ``admin_pac`` solely so the security probe happens before this one
    spins up.
    """
    policy = _psdk_client_policy(
        user=os.environ.get("AEROSPIKE_HOST_8_1_3_USER", "admin"),
        password=os.environ.get("AEROSPIKE_HOST_8_1_3_PASSWORD", "admin"),
    )
    async with Client(seeds=aerospike_host_8_1_3, policy=policy) as c:
        await asyncio.sleep(2)
        yield c


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def masking_setup(admin_pac, admin_client):
    """User management via PAC; masking-rule application via PSDK ``Session.info``."""
    for username in (_USER_READER, _USER_BASIC):
        try:
            await admin_pac.drop_user(username)
        except Exception:
            pass

    await admin_pac.create_user(_USER_READER, _USER_PASSWORD, ["read-write", "read-masked"])
    await _wait_for_user(admin_pac, _USER_READER)
    await admin_pac.create_user(_USER_BASIC, _USER_PASSWORD, ["read-write"])
    await _wait_for_user(admin_pac, _USER_BASIC)

    admin_sess = admin_client.create_session()
    await _apply_masking(
        admin_sess, ns=_NAMESPACE, set_name=_SET, bin_name=_BIN_MASKED, function="redact",
    )
    await _apply_masking(
        admin_sess, ns=_NAMESPACE, set_name=_SET, bin_name=_BIN_CONSTANT,
        function="constant", value="HIDDEN",
    )

    yield

    for bin_name in (_BIN_MASKED, _BIN_CONSTANT):
        await _remove_masking(admin_sess, ns=_NAMESPACE, set_name=_SET, bin_name=bin_name)
    for username in (_USER_READER, _USER_BASIC):
        try:
            await admin_pac.drop_user(username)
        except Exception:
            pass


def _psdk_client_policy(*, user: str, password: str) -> _PacClientPolicy:
    cp = _PacClientPolicy()
    cp.use_services_alternate = _services_alternate_813()
    cp.set_auth_mode(AuthMode.INTERNAL, user=user, password=password)
    return cp


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def reader_client(aerospike_host_8_1_3, masking_setup):
    """PSDK Client authenticated as ``psdk_strops_reader`` ([read-write, read-masked])."""
    policy = _psdk_client_policy(user=_USER_READER, password=_USER_PASSWORD)
    async with Client(seeds=aerospike_host_8_1_3, policy=policy) as c:
        await asyncio.sleep(2)
        yield c


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def basic_client(aerospike_host_8_1_3, masking_setup):
    """PSDK Client authenticated as ``psdk_strops_user`` ([read-write] only)."""
    policy = _psdk_client_policy(user=_USER_BASIC, password=_USER_PASSWORD)
    async with Client(seeds=aerospike_host_8_1_3, policy=policy) as c:
        await asyncio.sleep(2)
        yield c


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def reset_record(admin_client, masking_setup):
    """Reset the test record before every test via PSDK admin session.
    Retries transient failures up to 3 attempts.
    """
    sess = admin_client.create_session()
    record = {
        _BIN_MASKED: "hello world",
        _BIN_CONSTANT: "real-secret",
        _BIN_UNMASKED: "visible",
    }
    last_exc = None
    for attempt in range(3):
        try:
            await sess.put(_k(), record)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    yield


def _k():
    return _TEST_DS.id(_RECORD_KEY)


# ---------------------------------------------------------------------------
# Read-side scenarios — privilege gates which value the caller observes
# ---------------------------------------------------------------------------

class TestMaskingReads:

    async def test_privileged_str_strlen_returns_real_length(self, reader_client):
        """ReadMasked → real ``"hello world"`` length (11) via builder ``str_strlen``."""
        sess = reader_client.create_session()
        rs = await sess.query(_k()).bin(_BIN_MASKED).str_strlen().execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_MASKED] == 11

    async def test_privileged_str_substr_returns_real_prefix(self, reader_client):
        """ReadMasked → real prefix ``"hello"`` via builder ``str_substr``."""
        sess = reader_client.create_session()
        rs = await sess.query(_k()).bin(_BIN_MASKED).str_substr(0, 5).execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_MASKED] == "hello"

    async def test_unprivileged_str_substr_returns_redacted_prefix(self, basic_client):
        """Without ReadMasked → ``redact`` masking returns same-length stand-in."""
        sess = basic_client.create_session()
        rs = await sess.query(_k()).bin(_BIN_MASKED).str_substr(0, 5).execute()
        redacted = (await rs.first_or_raise()).record_or_raise().bins[_BIN_MASKED]
        assert isinstance(redacted, str)
        assert len(redacted) == 5
        assert redacted != "hello"

    async def test_unprivileged_str_find_returns_minus_one(self, basic_client):
        """Without ReadMasked → ``find("world")`` on redacted bin returns -1."""
        sess = basic_client.create_session()
        rs = await sess.query(_k()).bin(_BIN_MASKED).str_find("world").execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_MASKED] == -1

    async def test_unprivileged_str_contains_returns_false(self, basic_client):
        """Without ReadMasked → ``contains("hello")`` returns False on redacted bin."""
        sess = basic_client.create_session()
        rs = await sess.query(_k()).bin(_BIN_MASKED).str_contains("hello").execute()
        result = (await rs.first_or_raise()).record_or_raise().bins[_BIN_MASKED]
        assert result is False
        assert isinstance(result, bool)

    async def test_unmasked_bin_transparent_to_both_users(self, reader_client, basic_client):
        """The ``public`` bin (no masking) reads identically for both users."""
        reader_sess = reader_client.create_session()
        basic_sess = basic_client.create_session()
        rs1 = await reader_sess.query(_k()).bin(_BIN_UNMASKED).str_strlen().execute()
        rs2 = await basic_sess.query(_k()).bin(_BIN_UNMASKED).str_strlen().execute()
        len_priv = (await rs1.first_or_raise()).record_or_raise().bins[_BIN_UNMASKED]
        len_unpriv = (await rs2.first_or_raise()).record_or_raise().bins[_BIN_UNMASKED]
        assert len_priv == len_unpriv == 7  # len("visible")


# ---------------------------------------------------------------------------
# Modify-op gating — unprivileged user gets ROLE_VIOLATION
# ---------------------------------------------------------------------------

class TestMaskingModifiesBlocked:

    async def test_str_upper_blocked_for_unprivileged(self, basic_client):
        """``str_upper`` on a masked bin without WriteMasked → ROLE_VIOLATION."""
        sess = basic_client.create_session()
        with pytest.raises(Exception) as ei:
            await sess.upsert(_k()).bin(_BIN_MASKED).str_upper().execute()
        assert _is_role_violation(ei.value), f"expected ROLE_VIOLATION, got {ei.value!r}"

    async def test_str_concat_blocked_for_unprivileged(self, basic_client):
        """``str_concat`` on a masked bin without WriteMasked → ROLE_VIOLATION."""
        sess = basic_client.create_session()
        with pytest.raises(Exception) as ei:
            await sess.upsert(_k()).bin(_BIN_MASKED).str_concat("more").execute()
        assert _is_role_violation(ei.value), f"expected ROLE_VIOLATION, got {ei.value!r}"


# ---------------------------------------------------------------------------
# Privilege boundary — ReadMasked alone does NOT grant WriteMasked
# ---------------------------------------------------------------------------

class TestMaskingPrivilegeBoundary:

    async def test_read_masked_only_cannot_modify(self, reader_client):
        """ReadMasked is a READ privilege; modify still blocked → ROLE_VIOLATION."""
        sess = reader_client.create_session()
        with pytest.raises(Exception) as ei:
            await sess.upsert(_k()).bin(_BIN_MASKED).str_upper().execute()
        assert _is_role_violation(ei.value), f"expected ROLE_VIOLATION, got {ei.value!r}"


# ---------------------------------------------------------------------------
# Constant-mask variant — fixed replacement regardless of original length
# ---------------------------------------------------------------------------

class TestMaskingConstantFunction:

    async def test_constant_mask_split_view(self, reader_client, basic_client):
        """Privileged reader sees real (11 chars); unprivileged sees ``HIDDEN`` (6)."""
        reader_sess = reader_client.create_session()
        basic_sess = basic_client.create_session()

        # Privileged: real "real-secret" = 11 chars
        rs = await reader_sess.query(_k()).bin(_BIN_CONSTANT).str_strlen().execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_CONSTANT] == 11

        # Unprivileged: constant "HIDDEN" = 6 chars
        rs = await basic_sess.query(_k()).bin(_BIN_CONSTANT).str_strlen().execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_CONSTANT] == 6

        # Privileged substr(0,4): "real"
        rs = await reader_sess.query(_k()).bin(_BIN_CONSTANT).str_substr(0, 4).execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_CONSTANT] == "real"

        # Unprivileged substr(0,4): "HIDD"
        rs = await basic_sess.query(_k()).bin(_BIN_CONSTANT).str_substr(0, 4).execute()
        assert (await rs.first_or_raise()).record_or_raise().bins[_BIN_CONSTANT] == "HIDD"
