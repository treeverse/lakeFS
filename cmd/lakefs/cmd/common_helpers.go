package cmd

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/pkg/cmdutils"
)

// The messages below are rendered by pkg/cmdutils, the same output utility that
// lakectl uses, so both binaries share one implementation.
const (
	errorMessageTemplate = "Error: {{.Message}}\n"
	infoMessageTemplate  = "{{.Message|blue}}\n"
	warnMessageTemplate  = "{{.Message|red}}\n"
)

type messageData struct {
	Message string
}

func printMsgAndExit(params ...any) {
	cmdutils.WriteTo(errorMessageTemplate, messageData{Message: fmt.Sprint(params...)}, os.Stderr)
	os.Exit(1)
}

func printInfo(params ...any) {
	cmdutils.WriteTo(infoMessageTemplate, messageData{Message: "Info: " + fmt.Sprint(params...)}, os.Stderr)
}

func printNonFatalError(params ...any) {
	cmdutils.WriteTo(warnMessageTemplate, messageData{Message: "Error: " + fmt.Sprint(params...)}, os.Stderr)
}
