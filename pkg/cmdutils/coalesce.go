package cmdutils

import "reflect"

// Coalesce returns the first non-nil and non-empty string, or nil.  It returns an
// oddly-formatted result if the chosen value is neither string nor *string.
func Coalesce(values ...any) string {
	for _, value := range values {
		v := reflect.Indirect(reflect.ValueOf(value))
		if v.IsValid() && !v.IsZero() {
			return v.String()
		}
	}
	return ""
}
