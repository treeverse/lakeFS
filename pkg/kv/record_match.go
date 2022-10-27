package kv

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Record struct {
	Partition string      `json:"partition"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
}

type MatchRecord struct {
	PartitionPattern string
	PathPattern      string
	MessageType      protoreflect.MessageType
}

var recordsMatches []MatchRecord

var ErrPatternAlreadyRegistered = errors.New("pattern already registered")

// RegisterType - Register a pb message type to parse the data, according to a path regex
// All objects which match the path regex will be parsed as that type
// A nil type parses the value as a plain string
func RegisterType(partitionPattern, pathPattern string, mt protoreflect.MessageType) error {
	// verify we are not trying to add the same record twice
	for _, rec := range recordsMatches {
		if rec.PartitionPattern == partitionPattern && rec.PathPattern == pathPattern {
			return ErrPatternAlreadyRegistered
		}
	}

	// add the new record
	recordsMatches = append(recordsMatches, MatchRecord{
		PartitionPattern: partitionPattern,
		PathPattern:      pathPattern,
		MessageType:      mt,
	})

	// make sure that we keep the longest pattern first
	sort.Slice(recordsMatches, func(i, j int) bool {
		if recordsMatches[i].PartitionPattern == recordsMatches[j].PartitionPattern {
			return recordsMatches[i].PathPattern > recordsMatches[j].PathPattern
		}
		return recordsMatches[i].PartitionPattern > recordsMatches[j].PartitionPattern
	})
	return nil
}

func MustRegisterType(partitionPattern, pathPattern string, mt protoreflect.MessageType) {
	err := RegisterType(partitionPattern, pathPattern, mt)
	if err != nil {
		panic(fmt.Errorf("%w: partition '%s', path '%s'", err, partitionPattern, pathPattern))
	}
}

// FindMessageTypeRecord lookup proto message type based on the partition and path.
//
//	Can return nil in case the value is not matched
func FindMessageTypeRecord(partition, path string) protoreflect.MessageType {
	for _, r := range recordsMatches {
		partitionMatch := patternMatch(r.PartitionPattern, partition)
		if !partitionMatch {
			continue
		}

		pathMatch := patternMatch(r.PathPattern, path)
		if pathMatch {
			return r.MessageType
		}
	}
	return nil
}

// patternMatch reports str matches a pattern. The pattern uses '/' as separator of each part. Part can match specific value or any ('*').
// Match works as prefix match as it will verify the given pattern and return success when pattern is completed no matter of the value holds more parts.
//
// Examples:
//
//	patternMatch("repo", "repo") // true
//	patternMatch("repo", "repo/something") // true
//	patternMatch("repo", "repository") // false
//	patternMatch("*", "repository") // true
//	patternMatch("repo/*/branches", "repo") // false
//	patternMatch("repo/*/branches", "repo/branch1") // false
//	patternMatch("repo/*/branches", "repo/branch1/branches") // true
//	patternMatch("repo/*/branches", "repo/branch1/branches/objects") // true
func patternMatch(pattern string, str string) bool {
	pp := strings.Split(pattern, "/")
	v := strings.SplitN(str, "/", len(pp)+1)
	if len(v) < len(pp) {
		// value doesn't hold enough parts
		return false
	}
	// match each part to the pattern
	for i, p := range pp {
		if p != "*" && p != v[i] {
			return false
		}
	}
	return true
}

func NewRecord(partition, path string, rawValue []byte) (*Record, error) {
	mt := FindMessageTypeRecord(partition, path)

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
