package kv

import (
	"regexp"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var matchers = make(map[string]protoreflect.ProtoMessage)
var defaultMsg protoreflect.ProtoMessage = nil

type KvObject struct {
	Partition string
	Key       string
	Value     interface{}
}

// Register a pb message type to parse the data, according to a path regex
// All objects which match the path regex will be parsed as that type
// A nil type parses the value as a plain string
func RegisterType(pathRegexp string, pb protoreflect.ProtoMessage) {
	matchers[pathRegexp] = pb
}

// The pb message type to parse a value, in case the path does not meet
// any regex of the above. This is done mainly to support paths which
// have no rules and may cause a conflict with another regex
// Note that multiple calls ti RegisterDefaultType will override each other
func RegisterDefaultType(pb protoreflect.ProtoMessage) {
	defaultMsg = pb
}

func matchPath(path string, pathRegexp string) bool {
	match, err := regexp.MatchString(PathBeginRegexp+pathRegexp, path)
	return err == nil && match
}

func resolveKVPathToMsgType(path string) (protoreflect.ProtoMessage, error) {
	for pathRegexp, msg := range matchers {
		if matchPath(path, pathRegexp) {
			return msg, nil
		}
	}
	if defaultMsg == nil {
		return nil, ErrNotFound
	}
	return defaultMsg, nil
}

func ToKvObject(partition, path string, rawValue []byte) (*KvObject, error) {
	msg, err := resolveKVPathToMsgType(path)
	if err != nil {
		return nil, err
	}

	var value interface{} = rawValue
	if msg != nil {
		err = proto.Unmarshal(rawValue, msg)
		if err != nil {
			return nil, err
		}
		value = msg
	}

	return &KvObject{
		Partition: partition,
		Key:       path,
		Value:     value,
	}, nil
}
