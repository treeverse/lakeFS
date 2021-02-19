package cmd

import (
	"github.com/manifoldco/promptui"
	"github.com/spf13/pflag"
	"io"
	"io/ioutil"
	"os"
)

const (
	AutoConfirmFlagName     = "yes"
	AutoConfigFlagShortName = "y"
	AutoConfirmFlagHelp     = "Automatically say yes to all confirmations"

	StdinFileName = "-"
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

// OpenByPath returns a reader from the given path. If path is "-", it'll return Stdin
func OpenByPath(path string) io.ReadCloser {
	if path == StdinFileName {
		// read from stdin
		return ioutil.NopCloser(os.Stdin)
	}
	fp, err := os.Open(path)
	if err != nil {
		DieErr(err)
	}
	return fp
}

func MustString(v string, err error) string {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustInt(v int, err error) int {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustBool(v bool, err error) bool {
	if err != nil {
		DieErr(err)
	}
	return v
}
