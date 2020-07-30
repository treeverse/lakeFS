package retention

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/logging"
)

// Fields is a string-keyed map with a nice printed representation.
type Fields map[string]interface{}

func (f Fields) String() string {
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	sep := ""
	for _, k := range keys {
		b.WriteString(fmt.Sprintf("%s%s=%v", sep, k, f[k]))
		if sep == "" {
			sep = ", "
		}
	}
	return b.String()
}

func copyFields(f Fields) Fields {
	ret := Fields{}
	for k, v := range f {
		ret[k] = v
	}
	return ret
}

// WithField augments fields with another field.
func (f Fields) WithField(key string, value interface{}) Fields {
	ret := copyFields(f)
	ret[key] = value
	return ret
}

// WithFields merges two Fields.
func (f Fields) WithFields(g Fields) Fields {
	ret := copyFields(f)
	for k, v := range g {
		ret[k] = v
	}
	return ret
}

// FromLoggerContext returns Fields using logging keys from ctx.  This is not stealing: logging
// exports the field key.
func FromLoggerContext(ctx context.Context) Fields {
	ret := Fields{}
	loggerFields := ctx.Value(logging.LogFieldsContextKey)
	if loggerFields != nil {
		for k, v := range loggerFields.(logging.Fields) {
			ret[k] = v
		}
	}
	return ret
}

// MapError wraps an error and adds multiple keyed additional Fields of string-keyed
// information.
type MapError struct {
	Fields       Fields
	WrappedError error
}

func (m MapError) Unwrap() error {
	return m.WrappedError
}

func (m MapError) Error() string {
	return fmt.Sprintf("%+v %s", m.Fields, m.WrappedError)
}
