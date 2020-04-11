package db

import "fmt"

func Prefix(prefix string) string {
	// TODO: we need proper escaping here, at least for "%" but see if there's anything else
	return fmt.Sprintf("%s%%", prefix)
}
