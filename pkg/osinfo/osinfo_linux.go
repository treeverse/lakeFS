package osinfo

import (
	"bytes"
	"os/exec"
	"strings"
	"time"
)

func GetOSInfo() (OSInfo, error) {
	out, err := getInfo()
	for strings.Contains(out, brokenPipeOutput) {
		out, err = getInfo()
		time.Sleep(retryWaitTime)
	}
	osStr := replaceLineTerminations(out)
	osInfo := strings.Split(osStr, " ")
	oss := OSInfo{
		Version:  osInfo[1],
		Platform: osInfo[2],
		OS:       osInfo[3],
	}
	return oss, err
}

func getInfo() (string, error) {
	cmd := exec.Command("uname", "-srio")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	return out.String(), err
}
