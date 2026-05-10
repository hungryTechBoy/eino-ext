package langfuseotel

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

type groundedAnswerPayload struct {
	Answer           string   `json:"answer"`
	SelectedChunkIDs []string `json:"selected_chunk_ids"`
}

func convModelCallbackInput(in []callbacks.CallbackInput) []*model.CallbackInput {
	ret := make([]*model.CallbackInput, len(in))
	for i, c := range in {
		ret[i] = model.ConvCallbackInput(c)
	}
	return ret
}

func extractModelInput(ins []*model.CallbackInput) (config *model.Config, messages []*schema.Message, extra map[string]interface{}, err error) {
	var mas [][]*schema.Message
	for _, in := range ins {
		if in == nil {
			continue
		}
		if len(in.Messages) > 0 {
			mas = append(mas, in.Messages)
		}
		if len(in.Extra) > 0 {
			extra = in.Extra
		}
		if in.Config != nil {
			config = in.Config
		}
	}
	if len(mas) == 0 {
		return config, []*schema.Message{}, extra, nil
	}
	messages, err = concatMessageArray(mas)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("concat messages failed: %v", err)
	}
	return config, messages, extra, nil
}

func convModelCallbackOutput(out []callbacks.CallbackOutput) []*model.CallbackOutput {
	ret := make([]*model.CallbackOutput, len(out))
	for i, c := range out {
		ret[i] = model.ConvCallbackOutput(c)
	}
	return ret
}

func extractModelOutput(outs []*model.CallbackOutput) (usage *model.TokenUsage, message *schema.Message, extra map[string]interface{}, err error) {
	var mas []*schema.Message
	for _, out := range outs {
		if out == nil {
			continue
		}
		if out.TokenUsage != nil {
			usage = out.TokenUsage
		}
		if out.Message != nil {
			mas = append(mas, out.Message)
		}
		if out.Extra != nil {
			extra = out.Extra
		}
	}
	if len(mas) == 0 {
		return usage, &schema.Message{}, extra, nil
	}
	message, err = schema.ConcatMessages(mas)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("concat message failed: %v", err)
	}
	return usage, message, extra, nil
}

func concatMessageArray(mas [][]*schema.Message) ([]*schema.Message, error) {
	arrayLen := len(mas[0])
	ret := make([]*schema.Message, arrayLen)
	slicesToConcat := make([][]*schema.Message, arrayLen)

	for _, ma := range mas {
		if len(ma) != arrayLen {
			return nil, fmt.Errorf("unexpected array length. Got %d, expected %d", len(ma), arrayLen)
		}
		for i := 0; i < arrayLen; i++ {
			m := ma[i]
			if m != nil {
				slicesToConcat[i] = append(slicesToConcat[i], m)
			}
		}
	}

	for i, slice := range slicesToConcat {
		if len(slice) == 0 {
			ret[i] = nil
		} else if len(slice) == 1 {
			ret[i] = slice[0]
		} else {
			cm, err := schema.ConcatMessages(slice)
			if err != nil {
				return nil, err
			}
			ret[i] = cm
		}
	}

	return ret, nil
}

func getName(info *callbacks.RunInfo) string {
	if len(info.Name) != 0 {
		return info.Name
	}
	return info.Type + string(info.Component)
}

func extractOutputText(message *schema.Message) string {
	if message == nil {
		return ""
	}
	if answer, _, ok := extractGroundedAnswer(message); ok {
		return answer
	}
	return strings.TrimSpace(message.Content)
}

func extractObservationOutput(message *schema.Message) string {
	if message == nil {
		return ""
	}
	payload := map[string]any{}
	if role := strings.TrimSpace(string(message.Role)); role != "" {
		payload["role"] = role
	}
	if content := strings.TrimSpace(message.Content); content != "" {
		payload["content"] = content
	}
	if reasoning := strings.TrimSpace(message.ReasoningContent); reasoning != "" {
		payload["reasoning_content"] = reasoning
	}
	if message.ResponseMeta != nil {
		responseMeta := map[string]any{}
		if finishReason := strings.TrimSpace(message.ResponseMeta.FinishReason); finishReason != "" {
			responseMeta["finish_reason"] = finishReason
		}
		if message.ResponseMeta.Usage != nil {
			responseMeta["usage"] = message.ResponseMeta.Usage
		}
		if len(responseMeta) > 0 {
			payload["response_meta"] = responseMeta
		}
	}
	toolCalls := make([]map[string]any, 0, len(message.ToolCalls))
	for _, toolCall := range message.ToolCalls {
		item := map[string]any{}
		if toolCall.ID != "" {
			item["id"] = toolCall.ID
		}
		if toolCall.Type != "" {
			item["type"] = toolCall.Type
		}
		function := map[string]any{}
		if toolCall.Function.Name != "" {
			function["name"] = toolCall.Function.Name
		}
		if strings.TrimSpace(toolCall.Function.Arguments) != "" {
			var parsed any
			if err := json.Unmarshal([]byte(toolCall.Function.Arguments), &parsed); err == nil {
				function["arguments"] = parsed
			} else {
				function["arguments"] = toolCall.Function.Arguments
			}
		}
		if len(function) > 0 {
			item["function"] = function
		}
		toolCalls = append(toolCalls, item)
	}
	if len(toolCalls) > 0 {
		payload["tool_calls"] = toolCalls
	}
	if len(payload) == 0 {
		return extractOutputText(message)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return extractOutputText(message)
	}
	return string(raw)
}

func extractGroundedAnswer(message *schema.Message) (string, []string, bool) {
	if message == nil {
		return "", nil, false
	}
	for _, toolCall := range message.ToolCalls {
		if toolCall.Function.Name != "submit_grounded_answer" {
			continue
		}
		var payload groundedAnswerPayload
		if err := json.Unmarshal([]byte(toolCall.Function.Arguments), &payload); err != nil {
			continue
		}
		answer := strings.TrimSpace(payload.Answer)
		if answer == "" {
			continue
		}
		return answer, payload.SelectedChunkIDs, true
	}
	return "", nil, false
}

func extractObservationMetadata(message *schema.Message) map[string]string {
	metadata := make(map[string]string)
	if message == nil {
		return metadata
	}
	if message.ResponseMeta != nil {
		if finishReason := strings.TrimSpace(message.ResponseMeta.FinishReason); finishReason != "" {
			metadata["finish_reason"] = finishReason
		}
	}
	if requestID, ok := message.Extra["openai-request-id"].(string); ok && strings.TrimSpace(requestID) != "" {
		metadata["openai_request_id"] = strings.TrimSpace(requestID)
	}
	if _, chunkIDs, ok := extractGroundedAnswer(message); ok && len(chunkIDs) > 0 {
		if raw, err := json.Marshal(chunkIDs); err == nil {
			metadata["selected_chunk_ids"] = string(raw)
		}
	}
	return metadata
}
