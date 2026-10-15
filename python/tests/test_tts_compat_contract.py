"""Validate the shared compatibility inventory, not unimplemented SDK behavior."""

import copy
import json
from pathlib import Path

import jsonschema
import pytest

DIRECTORY = Path(__file__).parents[2] / "conformance/tts-compat"
CONTRACT = json.loads((DIRECTORY / "contract.json").read_text())
SCHEMA = json.loads((DIRECTORY / "schema.json").read_text())
INVALID = json.loads((DIRECTORY / "invalid.json").read_text())
LANGUAGES = {"python", "typescript", "go", "rust"}
FAMILIES = {f"C{number:02}" for number in range(1, 15)}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_presence(value):
    if isinstance(value, dict):
        if "state" in value:
            state = value["state"]
            require(
                isinstance(state, str) and state in {"omitted", "value", "clear"},
                "invalid presence state",
            )
            require(("value" in value) == (state == "value"), "invalid presence value")
        for child in value.values():
            validate_presence(child)
    elif isinstance(value, list):
        for child in value:
            validate_presence(child)


def validate_contract(contract):
    jsonschema.validate(contract, SCHEMA)
    require(set(contract["languages"]) == LANGUAGES, "all four SDKs required")
    require(len(contract["languages"]) == 4, "duplicate SDK")
    require(set(contract["families"]) == FAMILIES, "missing capability family")
    inventory = {item["id"]: item for item in contract["inventory"]}
    require(len(inventory) == len(contract["inventory"]), "duplicate inventory ID")
    for item in inventory.values():
        require(item["source"] in contract["sources"], "unknown inventory source")
    case_ids, covered, families = set(), set(), set()
    for case in contract["cases"]:
        require(case["id"] not in case_ids, "duplicate case ID")
        case_ids.add(case["id"])
        require(case["baseline"] in contract["baselines"], "unknown baseline")
        require(set(case["families"]) <= FAMILIES, "unknown capability family")
        families.update(case["families"])
        require(set(case["sources"]) <= contract["sources"].keys(), "unknown source")
        require(set(case["gates"]) <= contract["gates"].keys(), "unknown gate")
        for item in case["covers"]:
            require(item in inventory, "unknown inventory reference")
            require(inventory[item]["baseline"] == case["baseline"], "wrong plugin coverage")
            covered.add(item)
        if case["status"] == "unresolved":
            require(bool(case["gates"]), "unresolved case needs a gate")
            require(all(a["op"] == "pending" for a in case["expected"]), "invented answer")
            require(
                any(contract["gates"][g]["status"] == "open" for g in case["gates"]),
                "unresolved case needs an open gate",
            )
        else:
            require(all(a["op"] != "pending" for a in case["expected"]), "untracked question")
        validate_presence(case["given"])
    require(covered == inventory.keys(), "uncovered public argument")
    require(families == FAMILIES, "untested capability family")


def test_tts_compatibility_inventory():
    validate_contract(CONTRACT)


@pytest.mark.parametrize("mutation", INVALID, ids=lambda item: item["id"])
def test_tts_compatibility_inventory_rejects_invalid_contract(mutation):
    contract = copy.deepcopy(CONTRACT)
    path = mutation["path"].split("/")[1:]
    parent = contract
    for key in path[:-1]:
        parent = parent[int(key)] if isinstance(parent, list) else parent[key]
    key = int(path[-1]) if isinstance(parent, list) else path[-1]
    parent[key] = mutation["value"]
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        validate_contract(contract)
