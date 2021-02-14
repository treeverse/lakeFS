package cmd

import (
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/pflag"
)

const (
	AutoConfirmFlagName     = "yes"
	AutoConfigFlagShortName = "y"
	AutoConfirmFlagHelp     = "Automatically say yes to all confirmations"
)

func AssignAutoConfirmFlag(flags *pflag.FlagSet) {
	flags.BoolP(AutoConfirmFlagName, AutoConfigFlagShortName, false, AutoConfirmFlagHelp)
}

func Confirm(flags *pflag.FlagSet, question string) (bool, error) {
	yes, err := flags.GetBool(AutoConfirmFlagName)
	if err == nil && yes {
		// got auto confirm flag
		return true, nil
	}
	prm := promptui.Prompt{
		Label:     question,
		IsConfirm: true,
	}
	_, err = prm.Run()
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetReader returns a reader from the given path. If path is "-", it'll return Stdin
func GetReader(path string) io.ReadCloser {
	if strings.EqualFold(path, "-") {
		// upload from stdin
		return ioutil.NopCloser(os.Stdin)
	}
	fp, err := os.Open(path)
	if err != nil {
		DieErr(err)
	}
	return fp
}
