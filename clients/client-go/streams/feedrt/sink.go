package feedrt

import (
	"errors"
	"fmt"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// payloadOf: a pushed payload is an object; anything else travels as {value}.
func payloadOf(value interface{}) interface{} {
	if value == nil || !jsv.IsObject(value) {
		m := map[string]interface{}{}
		setDef(m, "value", value)
		if value == nil {
			m["value"] = nil
		}
		return m
	}
	return value
}

func requireQueue(name interface{}) (string, error) {
	s, ok := name.(string)
	if !ok || s == "" {
		return "", fmt.Errorf(".to(): a queue name is a non-empty string, got %s", jsonOf(name))
	}
	return s, nil
}

func jsonOf(v interface{}) string {
	if jsv.IsUndefined(v) {
		return "undefined"
	}
	b, err := jsv.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func requirePartition(partition interface{}, queue string) (string, error) {
	if f, ok := jsv.Num(partition); ok {
		return jsv.NumberString(f), nil
	}
	s, ok := partition.(string)
	if !ok || s == "" {
		return "", fmt.Errorf(".to(): the partition for %s is a non-empty string, got %s", queue, jsonOf(partition))
	}
	return s, nil
}

// QueueNamer is anything that names a queue (the client's QueueBuilder).
type QueueNamer interface{ Name() string }

var errQueueName = errors.New("Could not extract queue name from sink target. " +
	"Pass either a queen.queue(\"name\") builder or { queueName: \"name\" }.")

func extractQueueName(t interface{}) (string, error) {
	switch x := t.(type) {
	case string:
		return requireQueue(x)
	case QueueNamer:
		if n := x.Name(); n != "" {
			return n, nil
		}
	case map[string]interface{}:
		if n, ok := x["name"].(string); ok && n != "" {
			return n, nil
		}
		if n, ok := x["_queueName"].(string); ok {
			return n, nil
		}
		if n, ok := x["queueName"].(string); ok {
			return n, nil
		}
	}
	return "", errQueueName
}

// targetsFor is where one record goes: [{queue, partition}], empty to send it
// nowhere. partition is the target's own, else the sink's partition option,
// else fallback (the source partition, or the key of a window or a pattern).
func targetsFor(s *SinkSpec, value, msg, fallback interface{}) ([]Target, error) {
	var raw []interface{}
	if s.Route != nil {
		got, err := callValue(func() (interface{}, error) { return s.Route(value, msg) })
		if err != nil {
			return nil, err
		}
		if jsv.IsNullish(got) {
			return nil, nil
		}
		switch g := got.(type) {
		case []interface{}:
			raw = g
		case []string:
			for _, x := range g {
				raw = append(raw, x)
			}
		case []Target:
			for _, x := range g {
				raw = append(raw, x)
			}
		default:
			if jsv.IsArray(got) {
				if l, ok := jsv.Normalize(got).([]interface{}); ok {
					raw = l
					break
				}
			}
			raw = []interface{}{got}
		}
	} else {
		for _, q := range s.Queues {
			raw = append(raw, q)
		}
	}
	var out []Target
	for _, t := range raw {
		if jsv.IsNullish(t) {
			continue
		}
		var queue string
		var partition interface{}
		var err error
		switch x := t.(type) {
		case Target:
			if queue, err = requireQueue(x.Queue); err != nil {
				return nil, err
			}
			if x.Partition != "" {
				partition = x.Partition
			}
		case *Target:
			if queue, err = requireQueue(x.Queue); err != nil {
				return nil, err
			}
			if x.Partition != "" {
				partition = x.Partition
			}
		case map[string]interface{}:
			_, hasName := x["name"].(string)
			q, hasQueue := x["queue"].(string)
			if !hasName && hasQueue {
				if queue, err = requireQueue(q); err != nil {
					return nil, err
				}
				if p, ok := x["partition"]; ok && !jsv.IsNullish(p) {
					partition = p
				}
			} else if queue, err = extractQueueName(x); err != nil {
				return nil, err
			}
		default:
			if queue, err = extractQueueName(t); err != nil {
				return nil, err
			}
		}
		if partition == nil {
			if partition, err = resolvePartition(s, value, fallback); err != nil {
				return nil, err
			}
		}
		p, err := requirePartition(partition, queue)
		if err != nil {
			return nil, err
		}
		out = append(out, Target{Queue: queue, Partition: p})
	}
	return out, nil
}

func resolvePartition(s *SinkSpec, value, fallback interface{}) (interface{}, error) {
	if s.PartitionFn != nil {
		v, err := callValue(func() (interface{}, error) { return s.PartitionFn(value) })
		if err != nil {
			return nil, err
		}
		return jsv.String(v), nil
	}
	if s.HasPartition {
		return s.Partition, nil
	}
	if jsv.IsNullish(fallback) || fallback == "" {
		return "Default", nil
	}
	return jsv.String(fallback), nil
}
