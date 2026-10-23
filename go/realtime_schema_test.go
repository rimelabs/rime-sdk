package rime

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dlclark/regexp2"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// Prism uses Unicode escapes and lookahead, which Go's standard regexp lacks.
type rtSchemaRegexp struct{ *regexp2.Regexp }

func (r rtSchemaRegexp) MatchString(s string) bool {
	ok, err := r.Regexp.MatchString(s)
	return err == nil && ok
}
func rtSchemaCompiler() (*jsonschema.Compiler, map[string]any, error) {
	raw, err := os.ReadFile("testdata/prism/speech_to_speech.asyncapi.yaml")
	if err != nil {
		return nil, nil, err
	}
	var contract map[string]any
	if err = yaml.Unmarshal(raw, &contract); err != nil {
		return nil, nil, err
	}
	// Normalize YAML numbers to JSON values for the schema compiler.
	raw, err = json.Marshal(contract)
	if err != nil {
		return nil, nil, err
	}
	if err = json.Unmarshal(raw, &contract); err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7)
	compiler.UseRegexpEngine(func(pattern string) (jsonschema.Regexp, error) {
		re, err := regexp2.Compile(pattern, regexp2.ECMAScript)
		if err != nil {
			return nil, err
		}
		return rtSchemaRegexp{re}, nil
	})
	err = compiler.AddResource("urn:prism", map[string]any{"components": contract["components"]})
	return compiler, contract, err
}

var rtRequestSchemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	compiler, contract, err := rtSchemaCompiler()
	if err != nil {
		return nil, err
	}
	messages := contract["components"].(map[string]any)["messages"].(map[string]any)
	schemas := make(map[string]*jsonschema.Schema, len(messages))
	for key, value := range messages {
		schema, err := compiler.Compile("urn:prism#/components/messages/" + key + "/payload")
		if err != nil {
			return nil, err
		}
		schemas[value.(map[string]any)["name"].(string)] = schema
	}
	return schemas, nil
})

func rtValidateRequest(value rtObject) error {
	schemas, err := rtRequestSchemas()
	if err != nil {
		return err
	}
	kind, _ := value["type"].(string)
	schema := schemas[kind]
	if schema == nil {
		return fmt.Errorf("unknown request type %q", kind)
	}
	return schema.Validate(map[string]any(value))
}

func TestRealtimeSharedSchema(t *testing.T) {
	compiler, _, err := rtSchemaCompiler()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("testdata/prism/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name, Schema string
		Value        any
		Valid        bool
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			schema, err := compiler.Compile("urn:prism#/components/schemas/" + c.Schema)
			if err != nil {
				t.Fatal(err)
			}
			err = schema.Validate(c.Value)
			if (err == nil) != c.Valid {
				t.Fatalf("valid=%v: %v", c.Valid, err)
			}
		})
	}
}
