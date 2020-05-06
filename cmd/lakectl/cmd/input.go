package cmd

import (
	"net/url"

	"github.com/manifoldco/promptui"
)

func promptuiValidateURL(s string) error {
	if len(s) == 0 {
		return nil
	}
	_, err := url.ParseRequestURI(s)
	return err
}

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
