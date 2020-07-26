package cmd

import (
	"github.com/manifoldco/promptui"
	"github.com/spf13/pflag"
)

func confirm(flags *pflag.FlagSet, question string) (bool, error) {
	force, _ := flags.GetBool("force")
	if force {
		return true, nil
	}
	prm := promptui.Prompt{
		Label:     question,
		IsConfirm: true,
	}
	_, err := prm.Run()
	if err != nil {
		return false, err
	}
	return true, nil
}
