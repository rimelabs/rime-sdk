package rime

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// T0 validates the inventory, not the unimplemented transport behavior it describes.
type compatContract struct {
	SchemaVersion int                        `json:"schema_version"`
	Stage         string                     `json:"stage"`
	Languages     []string                   `json:"languages"`
	Baselines     map[string]json.RawMessage `json:"baselines"`
	Sources       map[string]json.RawMessage `json:"sources"`
	Families      map[string]string          `json:"families"`
	Gates         map[string]struct {
		Status string `json:"status"`
	} `json:"gates"`
	Inventory []struct {
		ID       string `json:"id"`
		Baseline string `json:"baseline"`
		Source   string `json:"source"`
	} `json:"inventory"`
	Cases []struct {
		ID              string            `json:"id"`
		Baseline        string            `json:"baseline"`
		Families        []string          `json:"families"`
		Sources         []string          `json:"sources"`
		Gates           []string          `json:"gates"`
		Covers          []string          `json:"covers"`
		Status          string            `json:"status"`
		Given           any               `json:"given"`
		SDKVerification map[string]string `json:"sdk_verification"`
		Expected        []struct {
			Op string `json:"op"`
		} `json:"expected"`
	} `json:"cases"`
}

func validateCompatPresence(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if rawState, present := value["state"]; present {
			state, ok := rawState.(string)
			_, hasValue := value["value"]
			if !ok || (state != "omitted" && state != "clear" && state != "value") || hasValue != (state == "value") {
				return fmt.Errorf("invalid presence marker")
			}
		}
		for _, child := range value {
			if err := validateCompatPresence(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := validateCompatPresence(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCompatContract(data []byte) error {
	var contract compatContract
	if err := json.Unmarshal(data, &contract); err != nil {
		return err
	}
	languages := map[string]bool{"python": true, "typescript": true, "go": true, "rust": true}
	if contract.SchemaVersion != 1 || contract.Stage != "T0" || len(contract.Languages) != 4 {
		return fmt.Errorf("invalid contract version, stage or languages")
	}
	seenLanguages := map[string]bool{}
	for _, language := range contract.Languages {
		if !languages[language] || seenLanguages[language] {
			return fmt.Errorf("invalid or duplicate language")
		}
		seenLanguages[language] = true
	}
	if len(contract.Families) != 14 {
		return fmt.Errorf("missing capability family")
	}
	for i := 1; i <= 14; i++ {
		if contract.Families[fmt.Sprintf("C%02d", i)] == "" {
			return fmt.Errorf("missing capability family")
		}
	}
	inventory := map[string]string{}
	for _, item := range contract.Inventory {
		if item.ID == "" || inventory[item.ID] != "" || contract.Sources[item.Source] == nil {
			return fmt.Errorf("invalid inventory entry")
		}
		inventory[item.ID] = item.Baseline
	}
	ids, covered, families := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, entry := range contract.Cases {
		if entry.ID == "" || ids[entry.ID] || contract.Baselines[entry.Baseline] == nil {
			return fmt.Errorf("invalid case identity")
		}
		ids[entry.ID] = true
		if len(entry.Families) == 0 || len(entry.Sources) == 0 || len(entry.Expected) == 0 {
			return fmt.Errorf("missing case evidence or expectations")
		}
		for _, family := range entry.Families {
			if contract.Families[family] == "" {
				return fmt.Errorf("unknown family")
			}
			families[family] = true
		}
		for _, source := range entry.Sources {
			if contract.Sources[source] == nil {
				return fmt.Errorf("unknown source")
			}
		}
		openGate := false
		for _, gate := range entry.Gates {
			value, ok := contract.Gates[gate]
			if !ok {
				return fmt.Errorf("unknown gate")
			}
			openGate = openGate || value.Status == "open"
		}
		for _, id := range entry.Covers {
			if inventory[id] != entry.Baseline {
				return fmt.Errorf("unknown or mismatched inventory reference")
			}
			covered[id] = true
		}
		if entry.Status != "specified" && entry.Status != "unresolved" {
			return fmt.Errorf("unknown case status")
		}
		if entry.Status == "unresolved" && !openGate {
			return fmt.Errorf("unresolved case needs an open gate")
		}
		for _, assertion := range entry.Expected {
			if (assertion.Op == "pending") != (entry.Status == "unresolved") {
				return fmt.Errorf("untracked question or invented answer")
			}
		}
		if len(entry.SDKVerification) != 4 {
			return fmt.Errorf("missing SDK verification state")
		}
		for language := range languages {
			if entry.SDKVerification[language] != "not_run" {
				return fmt.Errorf("T0 must not claim SDK conformance")
			}
		}
		if err := validateCompatPresence(entry.Given); err != nil {
			return err
		}
	}
	if len(inventory) == 0 || len(covered) != len(inventory) || len(families) != 14 {
		return fmt.Errorf("incomplete inventory or family coverage")
	}
	return nil
}

func readCompatFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/tts-compat/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestTTSCompatContract(t *testing.T) {
	data := readCompatFixture(t, "contract")
	if err := validateCompatContract(data); err != nil {
		t.Fatal(err)
	}
	// Python/Node apply the full JSON Schema. Go checks typed loading and the
	// shared cross-reference/presence invariants without adding a dependency.
	if !json.Valid(readCompatFixture(t, "schema")) {
		t.Fatal("invalid JSON Schema")
	}
	var mutations []struct {
		ID    string `json:"id"`
		Path  string `json:"path"`
		Value any    `json:"value"`
	}
	if err := json.Unmarshal(readCompatFixture(t, "invalid"), &mutations); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range mutations {
		t.Run(mutation.ID, func(t *testing.T) {
			var invalid any
			if err := json.Unmarshal(data, &invalid); err != nil {
				t.Fatal(err)
			}
			path := strings.Split(strings.TrimPrefix(mutation.Path, "/"), "/")
			parent := invalid
			for i, key := range path {
				switch node := parent.(type) {
				case map[string]any:
					if i == len(path)-1 {
						node[key] = mutation.Value
					} else {
						parent = node[key]
					}
				case []any:
					index, err := strconv.Atoi(key)
					if err != nil || index < 0 || index >= len(node) {
						t.Fatalf("invalid mutation path: %s", mutation.Path)
					}
					if i == len(path)-1 {
						node[index] = mutation.Value
					} else {
						parent = node[index]
					}
				default:
					t.Fatalf("invalid mutation path: %s", mutation.Path)
				}
			}
			encoded, err := json.Marshal(invalid)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateCompatContract(encoded); err == nil {
				t.Fatal("accepted invalid contract")
			}
		})
	}
}
