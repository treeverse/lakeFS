package cmd

import (
	"fmt"
	"os"
)

func printMsgAndExit(params ...any) {
	fmt.Fprintln(os.Stderr, "Error:", fmt.Sprint(params...))
	os.Exit(1)
}
