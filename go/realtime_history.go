package rime

import (
	"context"
	"crypto/sha256"
	"unicode/utf8"
)

// HistoryItemType identifies one kind of conversation record.
type HistoryItemType string

const (
	HistoryMessage        HistoryItemType = "message"
	HistoryFunctionCall   HistoryItemType = "function_call"
	HistoryFunctionResult HistoryItemType = "function_call_output"
)

// HistoryItem contains a message, past tool call, or tool result, without audio.
// ID is optional on creation and present when read. Role is user, assistant, or system.
// Arguments and Output retain the original strings. A past call never executes a tool.
type HistoryItem struct {
	Type                                            HistoryItemType
	ID, Role, Text, CallID, Name, Arguments, Output string
}

func rtHistoryItem(item HistoryItem) (rtObject, error) {
	v := rtObject{"type": string(item.Type)}
	if item.ID != "" {
		if !utf8.ValidString(item.ID) || utf8.RuneCountInString(item.ID) > 32 {
			return nil, failure(ErrInput, "item ID must contain 1 to 32 UTF-8 characters")
		}
		v["id"] = item.ID
	}
	switch item.Type {
	case HistoryMessage:
		if item.Role != "user" && item.Role != "assistant" && item.Role != "system" {
			return nil, failure(ErrInput, "invalid history role")
		}
		if utf8.RuneCountInString(item.Text) > 4000 {
			return nil, failure(ErrInput, "history text exceeds 4000 characters")
		}
		v["role"] = item.Role
		v["text"] = item.Text
	case HistoryFunctionCall:
		if item.CallID == "" || item.Name == "" {
			return nil, failure(ErrInput, "past calls need a call ID and name")
		}
		v["call_id"] = item.CallID
		v["name"] = item.Name
		v["arguments"] = item.Arguments
	case HistoryFunctionResult:
		if item.CallID == "" {
			return nil, failure(ErrInput, "tool results need a call ID")
		}
		v["call_id"] = item.CallID
		v["output"] = item.Output
	default:
		return nil, failure(ErrInput, "invalid history item type")
	}
	if !rtJSONStringsValid(v) {
		return nil, failure(ErrInput, "history fields must be valid UTF-8")
	}
	return v, nil
}
func (decoder *rtDecoder) historyItem(value any) HistoryItem {
	v := decoder.object(value)
	item := HistoryItem{Type: HistoryItemType(decoder.string(v["type"])), ID: decoder.id(v["id"])}
	switch item.Type {
	case HistoryMessage:
		item.Role = decoder.string(v["role"])
		item.Text = decoder.string(v["text"])
		if item.Role != "user" && item.Role != "assistant" && item.Role != "system" {
			decoder.bad("history role")
		}
	case HistoryFunctionCall:
		item.CallID = decoder.id(v["call_id"])
		item.Name = decoder.id(v["name"])
		item.Arguments = decoder.string(v["arguments"])
	case HistoryFunctionResult:
		item.CallID = decoder.id(v["call_id"])
		item.Output = decoder.string(v["output"])
	default:
		decoder.bad("history item type")
	}
	return item
}

// GetHistory reads the items the next reply would see, in server order.
func (s *RealtimeSession) GetHistory(ctx context.Context) ([]HistoryItem, error) {
	r, err := s.invoke(ctx, &rtCommand{kind: "history_get", body: rtObject{}})
	return r.history, err
}

// DeleteItem hides an item from future replies. A call and its result are hidden together.
// The server accepts an already deleted item and rejects an unknown ID.
func (s *RealtimeSession) DeleteItem(ctx context.Context, itemID string) error {
	if itemID == "" || !utf8.ValidString(itemID) {
		return failure(ErrInput, "item ID must be nonempty UTF-8")
	}
	_, err := s.invoke(ctx, &rtCommand{kind: "history_delete", target: itemID, body: rtObject{"item_id": itemID}})
	return err
}

// CreateItem adds history without a reply. Empty previousItemID appends, root inserts
// first, and an item ID inserts after its whole turn. Tool results cannot take an anchor.
func (s *RealtimeSession) CreateItem(ctx context.Context, item HistoryItem, previousItemID string) (ItemRef, error) {
	value, err := rtHistoryItem(item)
	if err != nil {
		return ItemRef{}, err
	}
	body := rtObject{"item": value}
	if previousItemID != "" {
		if !utf8.ValidString(previousItemID) || item.Type == HistoryFunctionResult {
			return ItemRef{}, failure(ErrInput, "invalid history anchor")
		}
		body["previous_item_id"] = previousItemID
	}
	c := &rtCommand{kind: "history", body: body, target: item.ID}
	if item.Type == HistoryFunctionResult {
		c.kind = "history_result"
		c.target = item.CallID
		c.digest = sha256.Sum256([]byte(item.Output))
	}
	result, err := s.invoke(ctx, c)
	return result.item, err
}
