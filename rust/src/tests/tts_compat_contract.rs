//! Inventory checks only: T0 does not execute the proposed SDK transports.
use serde_json::Value;
use std::collections::BTreeSet;

const CONTRACT: &str = include_str!("../../testdata/tts-compat/contract.json");
const SCHEMA: &str = include_str!("../../testdata/tts-compat/schema.json");
const INVALID: &str = include_str!("../../testdata/tts-compat/invalid.json");

fn require(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn strings(value: &Value) -> Result<BTreeSet<&str>, String> {
    value
        .as_array()
        .ok_or("expected string array")?
        .iter()
        .map(|item| item.as_str().ok_or_else(|| "expected string".to_owned()))
        .collect()
}

fn presence(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(state) = object.get("state") {
                require(
                    matches!(state.as_str(), Some("omitted" | "clear" | "value"))
                        && object.contains_key("value") == (state == "value"),
                    "invalid presence marker",
                )?;
            }
            for child in object.values() {
                presence(child)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                presence(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate(contract: &Value) -> Result<(), String> {
    require(
        contract["schema_version"] == 1 && contract["stage"] == "T0",
        "invalid version or stage",
    )?;
    let languages = BTreeSet::from(["python", "typescript", "go", "rust"]);
    require(
        strings(&contract["languages"])? == languages
            && contract["languages"].as_array().map(Vec::len) == Some(4),
        "all four SDKs required without duplicates",
    )?;
    let families: BTreeSet<_> = (1..=14).map(|n| format!("C{n:02}")).collect();
    let actual_families: BTreeSet<_> = contract["families"]
        .as_object()
        .ok_or("missing families")?
        .keys()
        .cloned()
        .collect();
    require(actual_families == families, "missing capability family")?;
    let inventory = contract["inventory"]
        .as_array()
        .ok_or("missing inventory")?;
    let mut inventory_ids = BTreeSet::new();
    for item in inventory {
        require(
            inventory_ids.insert(item["id"].as_str().ok_or("missing inventory ID")?),
            "duplicate inventory ID",
        )?;
        require(
            contract["sources"]
                .get(item["source"].as_str().ok_or("missing source")?)
                .is_some(),
            "unknown inventory source",
        )?;
    }
    require(!inventory_ids.is_empty(), "empty inventory")?;
    let mut ids = BTreeSet::new();
    let mut covered = BTreeSet::new();
    let mut seen_families = BTreeSet::new();
    for entry in contract["cases"].as_array().ok_or("missing cases")? {
        require(
            ids.insert(entry["id"].as_str().ok_or("missing case ID")?),
            "duplicate case ID",
        )?;
        require(
            contract["baselines"]
                .get(entry["baseline"].as_str().ok_or("missing baseline")?)
                .is_some(),
            "unknown baseline",
        )?;
        let case_families = strings(&entry["families"])?;
        require(!case_families.is_empty(), "missing case families")?;
        for family in case_families {
            require(families.contains(family), "unknown family")?;
            seen_families.insert(family.to_owned());
        }
        let sources = strings(&entry["sources"])?;
        require(!sources.is_empty(), "missing case sources")?;
        for source in sources {
            require(contract["sources"].get(source).is_some(), "unknown source")?;
        }
        let mut open_gate = false;
        for gate in strings(&entry["gates"])? {
            require(contract["gates"].get(gate).is_some(), "unknown gate")?;
            open_gate |= contract["gates"][gate]["status"] == "open";
        }
        for id in strings(&entry["covers"])? {
            let item = inventory
                .iter()
                .find(|item| item["id"] == id)
                .ok_or("unknown inventory reference")?;
            require(
                item["baseline"] == entry["baseline"],
                "wrong plugin coverage",
            )?;
            covered.insert(id);
        }
        let status = entry["status"].as_str().ok_or("missing case status")?;
        require(
            matches!(status, "specified" | "unresolved"),
            "unknown case status",
        )?;
        require(
            status != "unresolved" || open_gate,
            "unresolved case needs an open gate",
        )?;
        let expected = entry["expected"].as_array().ok_or("missing expectations")?;
        require(!expected.is_empty(), "empty expectations")?;
        for assertion in expected {
            require(
                (assertion["op"] == "pending") == (status == "unresolved"),
                "untracked question or invented answer",
            )?;
        }
        let verification = entry["sdk_verification"]
            .as_object()
            .ok_or("missing SDK verification")?;
        require(verification.len() == 4, "missing SDK verification state")?;
        for language in &languages {
            require(
                verification.get(*language).is_some_and(|v| v == "not_run"),
                "T0 must not claim SDK conformance",
            )?;
        }
        presence(&entry["given"])?;
    }
    require(covered == inventory_ids, "uncovered public argument")?;
    require(seen_families == families, "untested capability family")
}

#[test]
fn inventory_is_complete_without_claiming_sdk_conformance() {
    // Full JSON Schema validation runs in Python/Node. Rust loads the packaged
    // schema and checks the shared integrity rules without a new dependency.
    serde_json::from_str::<Value>(SCHEMA).unwrap();
    validate(&serde_json::from_str(CONTRACT).unwrap()).unwrap();
}

#[test]
fn shared_invalid_contracts_are_rejected() {
    let contract: Value = serde_json::from_str(CONTRACT).unwrap();
    let mutations: Value = serde_json::from_str(INVALID).unwrap();
    for mutation in mutations.as_array().unwrap() {
        let mut invalid = contract.clone();
        *invalid
            .pointer_mut(mutation["path"].as_str().unwrap())
            .expect("invalid mutation path") = mutation["value"].clone();
        assert!(validate(&invalid).is_err(), "accepted {}", mutation["id"]);
    }
}
