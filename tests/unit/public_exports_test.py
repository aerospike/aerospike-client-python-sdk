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

"""The SDK re-exports every PNC type its own API takes, so callers import one package."""

import inspect
import re
from pathlib import Path

import aerospike_native
import pytest
from aerospike_native import _native

import aerospike_sdk
import aerospike_sdk.policy

POLICY_ENUMS = ["CommitLevel", "ReadModeAP", "ReadModeSC", "Replica"]

OPERATION_BUILDERS = [
    "BitOperation",
    "CdtOperation",
    "ExpOperation",
    "HllOperation",
    "ListOperation",
    "MapOperation",
    "Operation",
    "StringOperation",
]

# Flags the builders take through untyped parameters, which the signature
# walk below cannot see.
UNTYPED_ARGUMENT_TYPES = ["ExpReadFlags", "ExpWriteFlags"]


@pytest.mark.parametrize("name", POLICY_ENUMS)
def test_policy_enum_is_the_pnc_class(name):
    pnc_class = getattr(aerospike_native, name)
    assert getattr(aerospike_sdk, name) is pnc_class
    assert getattr(aerospike_sdk.policy, name) is pnc_class
    assert name in aerospike_sdk.__all__
    assert name in aerospike_sdk.policy.__all__


@pytest.mark.parametrize("name", ["GeoJSON", *OPERATION_BUILDERS, *UNTYPED_ARGUMENT_TYPES])
def test_export_is_the_pnc_class(name):
    assert getattr(aerospike_sdk, name) is getattr(aerospike_native, name)
    assert name in aerospike_sdk.__all__


def _pnc_classes() -> dict:
    return {name: cls for name, cls in vars(_native).items() if inspect.isclass(cls)}


def _parameter_types(class_name: str, stub: str, classes: dict) -> dict:
    """PNC classes named in *class_name*'s method parameters, with a method that names each."""
    block = re.search(rf"^class {class_name}\b.*?(?=^class |^def |\Z)", stub, re.S | re.M)
    named: dict = {}
    if block is None:
        return named
    for method, parameters in re.findall(r"def (\w+)\((.*?)\) ->", block.group(0)):
        for word in re.findall(r"\b[A-Za-z_]\w*\b", parameters):
            if word in classes and word != class_name:
                named.setdefault(word, f"{class_name}.{method}")
    return named


def test_every_type_the_exported_classes_take_is_exported():
    """Walks every exported PNC class's signatures, and those of each type they name.

    A class exported without a type one of its methods requires still sends
    the caller to the native package. Exported means reachable by identity, so
    a renamed re-export counts.
    """
    # Older PNC stubs qualify their own types with a ``_native.`` prefix.
    stub = Path(aerospike_native.__file__).with_name("_native.pyi").read_text(encoding="utf-8")
    stub = re.sub(r"(?<![\w.])_native\.", "", stub)
    classes = _pnc_classes()
    exported = {id(value) for value in vars(aerospike_sdk).values()}
    roots = sorted(name for name, cls in classes.items() if id(cls) in exported)
    assert set(OPERATION_BUILDERS) <= set(roots)

    needed_by: dict = {}
    seen = set(roots)
    pending = list(roots)
    while pending:
        for name, user in _parameter_types(pending.pop(), stub, classes).items():
            needed_by.setdefault(name, user)
            if name not in seen:
                seen.add(name)
                pending.append(name)

    assert needed_by, "the stub walk found no parameter types"
    missing = {name: needed_by[name] for name in sorted(seen) if id(classes[name]) not in exported}
    assert not missing, f"not exported from aerospike_sdk (name: a method that takes it): {missing}"
