package internal

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/hashicorp/go-plugin"
)

const registeredPluginName = "plugin"

var errPanic = errors.New("")

var basicHS = PluginHandshake{
	Key:   "key",
	Value: "value",
}

type testCase struct {
	m           *Manager[PingPongStub]
	name        string
	p           plugin.Plugin
	expectedErr error
	description string
}

type testCases []testCase

func TestManager_LoadPluginClient(t *testing.T) {
	id := PluginIdentity{
		ProtocolVersion: 1,
	}
	cases := testCases{
		{
			NewManager[PingPongStub](),
			registeredPluginName,
			GRPCPlugin{Impl: &PingPongPlayer{}},
			nil,
			"successful registration",
		},
		{
			&Manager[PingPongStub]{},
			registeredPluginName,
			GRPCPlugin{Impl: &PingPongPlayer{}},
			errPanic,
			"uninitialized manager",
		},
		{
			NewManager[PingPongStub](),
			"someNonExistingPlugin",
			GRPCPlugin{Impl: &PingPongPlayer{}},
			ErrPluginNotFound,
			"unregistered service",
		},
		{
			NewManager[PingPongStub](),
			registeredPluginName,
			NopGRPCPlugin{},
			ErrPluginOfWrongType,
			"wrong type plugin",
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			if errors.Is(tc.expectedErr, errPanic) {
				defer func(c testCase) {
					if r := recover(); r == nil {
						t.Errorf("'%s' failed: should have paniced", c.description)
					}
				}(tc)
			}
			// Necessary to redefine in every test due to future setting of stdout by the go-plugin package
			loc, args, envVars := PluginServerCmd(id.ProtocolVersion, basicHS.Key, basicHS.Value)
			id.ExecutableLocation = loc
			id.ExecutableArgs = args
			id.ExecutableEnvVars = envVars
			hcProps := HCPluginProperties{
				ID:        id,
				Handshake: basicHS,
				P:         tc.p,
			}
			if tc.p != nil {
				tc.m.RegisterPlugin(registeredPluginName, hcProps)
			}
			_, closeClient, err := tc.m.LoadPluginClient(tc.name)
			assertErr(t, err, tc.expectedErr, tc.description)
			if closeClient != nil {
				closeClient()
				// validate that after closing a Client it can be used again.
				_, closeClient, err = tc.m.LoadPluginClient(tc.name)
				defer closeClient()
				assertErr(t, err, tc.expectedErr, tc.description)
			}
		})
	}
}

func assertErr(t *testing.T, err, cmpErr error, desc string) {
	if !errors.Is(cmpErr, errPanic) && err != nil && !errors.Is(err, cmpErr) {
		t.Errorf("'%s' failed: %v", desc, err)
	}
}

// Used to run the plugin server
func PluginServerCmd(version uint, key string, value string) (string, []string, []string) {
	cs := []string{"--plugin_server=true", key, value, strconv.Itoa(int(version))}
	cmd := exec.Command(os.Args[0], cs...)
	cmd.Env = os.Environ()
	return cmd.Path, cmd.Args[1:], cmd.Env
}

func TestMain(m *testing.M) {
	pluginServer := flag.Bool("plugin_server", false, "Run a plugin server")
	flag.Parse()

	// If the tests were triggered using the "--plugin_server" flag, then run the pluginServer instead of the tests.
	if *pluginServer {
		args := flag.Args()
		key := args[0]
		value := args[1]
		version := args[2]
		v, err := strconv.Atoi(version)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}
		RunPluginServer(key, value, v)
	} else {
		m.Run()
	}
}
