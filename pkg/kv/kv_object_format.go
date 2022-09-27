package kv

import (
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var matchers = make(map[string]protoreflect.ProtoMessage)
var defaultMsg protoreflect.ProtoMessage = nil

// Register a pb message type to parse the data, according to a path regex
// All objects which match the path regex will be parsed as that type
// A nil type parses the value as a plain string
func RegisterType(pathPrefix string, pb protoreflect.ProtoMessage) {
	matchers[pathPrefix] = pb
}

// The pb message type to parse a value, in case the path does not meet
// any regex of the above. This is done mainly to support paths which
// have no rules and may cause a conflict with another regex
// Note that multiple calls ti RegisterDefaultType will override each other
func RegisterDefaultType(pb protoreflect.ProtoMessage) {
	defaultMsg = pb
}

func matchPath(path string, pathPrefix string) bool {
	return strings.HasPrefix(path, pathPrefix)
}

func resolveKVPathToMsgType(path string) (protoreflect.ProtoMessage, error) {
	for pathPrefix, msg := range matchers {
		if matchPath(path, pathPrefix) {
			return msg, nil
		}
	}
	if defaultMsg == nil {
		return nil, ErrNotFound
	}
	return defaultMsg, nil
}

func ToPrettyString(path string, rawValue []byte) (string, error) {
	msg, err := resolveKVPathToMsgType(path)
	if err != nil {
		return "", err
	}

	if msg == nil {
		return string(rawValue), nil
	}

	err = proto.Unmarshal(rawValue, msg)
	if err != nil {
		return "", err
	}
	return protojson.Format(msg), nil
}
