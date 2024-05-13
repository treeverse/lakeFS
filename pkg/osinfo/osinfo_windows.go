package osinfo

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

func GetOSInfo() (OSInfo, error) {
	cmd := exec.Command("cmd", "ver")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		oss := OSInfo{
			OS:       "windows",
			Platform: "unknown",
		}
		return oss, fmt.Errorf("GetOSStats: %s", err)
	}
	osStr := replaceLineTerminations(out.String())
	verOpen := strings.Index(osStr, "[Version")
	verClose := strings.Index(osStr, "]")
	ver := "unknown"
	if verOpen != -1 && verClose != -1 {
		ver = osStr[verOpen+9 : verClose]
	}
	oss := OSInfo{
		OS:       "windows",
		Version:  ver,
		Platform: "unknown",
	}
	return oss, err
}
