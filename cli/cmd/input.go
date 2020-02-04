package cmd

import (
	"net/url"
	"strings"

	"github.com/manifoldco/promptui"
)

func prompt(question string) (string, error) {
	prm := promptui.Prompt{
		Label: question,
	}
	response, err := prm.Run()
	return response, err
}

func confirm(question string) (bool, error) {
	prm := promptui.Prompt{
		Label:     question,
		IsConfirm: true,
	}
	response, err := prm.Run()
	if err != nil {
		return false, err
	}
	return strings.EqualFold(strings.ToLower(response), "y") ||
		strings.EqualFold(strings.ToLower(response), "yes"), nil
}

func promptSecret(question string) (string, error) {
	prm := promptui.Prompt{
		Label:    question,
		Validate: nil,
		Mask:     '*',
	}
	response, err := prm.Run()
	return response, err
}

func promptUrl(question string) (string, error) {
	prm := promptui.Prompt{
		Label: question,
		Validate: func(s string) error {
			if len(s) == 0 {
				return nil
			}
			_, err := url.ParseRequestURI(s)
			return err
		},
	}
	response, err := prm.Run()
	return response, err
}
