package cmd

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/pkg/cmdutils"
)

const (
	errorMessageTemplate = "Error: {{.Message}}\n"
	infoMessageTemplate  = "Info: {{.Message|blue}}\n"
	warnMessageTemplate  = "Error: {{.Message|red}}\n"
)

type messageData struct {
	Message string
}

func printMsgAndExit(params ...any) {
	printMessage(errorMessageTemplate, params...)
	os.Exit(1)
}

func printInfo(params ...any) {
	printMessage(infoMessageTemplate, params...)
}

func printNonFatalError(params ...any) {
	printMessage(warnMessageTemplate, params...)
}

func printMessage(tpl string, params ...any) {
	cmdutils.WriteTo(tpl, messageData{Message: fmt.Sprint(params...)}, os.Stderr)
}
