package cmd

import (
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/text"
)

func printMsgAndExit(params ...any) {
	fmt.Fprintln(os.Stderr, "Error:", fmt.Sprint(params...))
	os.Exit(1)
}

func printInfo(params ...any) {
	colorMsg := text.FgHiBlue.Sprintf("Info: %s", fmt.Sprint(params...))
	fmt.Fprintln(os.Stderr, colorMsg)
}

func printNonFatalError(params ...any) {
	colorMsg := text.FgHiRed.Sprintf("Error: %s", fmt.Sprint(params...))
	fmt.Fprintln(os.Stderr, colorMsg)
}
