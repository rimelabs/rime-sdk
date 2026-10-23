use jsonschema::{Draft, Validator};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::OnceLock};

fn contract() -> Value {
    serde_yaml::from_str(include_str!(
        "../../../testdata/prism/speech_to_speech.asyncapi.yaml"
    ))
    .unwrap()
}
fn compile(schema: &Value) -> Validator {
    jsonschema::options()
        .with_draft(Draft::Draft7)
        .build(schema)
        .unwrap()
}
fn request_schemas() -> &'static HashMap<String, Validator> {
    static SCHEMAS: OnceLock<HashMap<String, Validator>> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        let contract = contract();
        contract["components"]["messages"]
            .as_object()
            .unwrap()
            .values()
            .map(|message| {
                let mut schema = message["payload"].clone();
                schema["components"] = contract["components"].clone();
                (
                    message["name"].as_str().unwrap().to_owned(),
                    compile(&schema),
                )
            })
            .collect()
    })
}
pub(super) fn prepare() {
    request_schemas();
}
pub(super) fn validate_request(value: &Value) {
    let schemas = request_schemas();
    let kind = value["type"].as_str().unwrap();
    let errors: Vec<_> = schemas[kind]
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{kind}: {}", errors.join("; "));
}
#[test]
fn shared_schema() {
    let contract = contract();
    let cases: Value =
        serde_json::from_str(include_str!("../../../testdata/prism/schema.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let schema = json!({"$ref":format!("#/components/schemas/{}",case["schema"].as_str().unwrap()),"components":contract["components"]});
        assert_eq!(
            compile(&schema).is_valid(&case["value"]),
            case["valid"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}
