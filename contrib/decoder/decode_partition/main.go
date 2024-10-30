// decode_partition decodes a lakeFS partition (the path component after
// "data/" in an external location) and extracts its time.  It may be useful
// for debugging.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/xid"
)

const (
	unixYear4000 = 64060588800
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s ID", os.Args[0])
		os.Exit(1)
	}

	xid, err := xid.FromString(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Not an XID: %s\n", err)
		os.Exit(1)
	}

	createdAt := xid.Time().UTC()
	fmt.Printf("%s (asc)\n", createdAt.UTC())
	createdAt = time.Unix(unixYear4000-int64(createdAt.Unix()), 0)
	fmt.Printf("%s (desc)\n", createdAt.UTC())
}
