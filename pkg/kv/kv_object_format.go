package kv

import (
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Record struct {
	Partition string
	Key       string
	Value     interface{}
}

type MatchRecord struct {
	PartitionPattern string
	PathPattern      string
	MessageType      protoreflect.MessageType
}

var recordsMatches []MatchRecord

// RegisterType - Register a pb message type to parse the data, according to a path regex
// All objects which match the path regex will be parsed as that type
// A nil type parses the value as a plain string
func RegisterType(partitionPattern, pathPattern string, mt protoreflect.MessageType) {
	recordsMatches = append(recordsMatches, MatchRecord{
		PartitionPattern: partitionPattern,
		PathPattern:      pathPattern,
		MessageType:      mt,
	})
	sort.Slice(recordsMatches, func(i, j int) bool {
		p1 := strings.TrimPrefix(recordsMatches[i].PartitionPattern, "*")
		p2 := strings.TrimPrefix(recordsMatches[j].PartitionPattern, "*")
		if p1 == p2 {
			pp1 := strings.TrimPrefix(recordsMatches[i].PathPattern, "*")
			pp2 := strings.TrimPrefix(recordsMatches[j].PathPattern, "*")
			return pp1 > pp2
		}
		return p1 > p2
	})
}

func NewRecord(partition, path string, rawValue []byte) (*Record, error) {
	var mt protoreflect.MessageType
	for _, r := range recordsMatches {
		partitionMatch, err := filepath.Match(r.PartitionPattern, partition)
		if err != nil {
			return nil, err
		}
		if !partitionMatch {
			continue
		}

		pathMatch, err := filepath.Match(r.PathPattern, path)
		if err != nil {
			return nil, err
		}
		if pathMatch {
			mt = r.MessageType
			break
		}
	}

	var value interface{}
	if mt == nil {
		value = rawValue
	} else {
		msg := mt.New().Interface()
		err := proto.Unmarshal(rawValue, msg)
		if err != nil {
			return nil, err
		}
		value = msg
	}

	return &Record{
		Partition: partition,
		Key:       path,
		Value:     value,
	}, nil
}
