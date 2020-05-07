package cmd

import (
	"github.com/manifoldco/promptui"
)

func confirm(question string) (bool, error) {
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
