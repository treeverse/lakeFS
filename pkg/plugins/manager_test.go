package plugins

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/hashicorp/go-plugin"
)

const registeredPluginName = "plugin"

var basicHS = PluginHandshake{
	Key:   "key",
	Value: "value",
}

type testCase struct {
	m           *Manager[PingPongStub]
	name        string
	p           plugin.Plugin
	err         error
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
			ErrUninitializedManager,
			"uninitialized manager",
		},
		{
			NewManager[PingPongStub](),
			"someNonExistingPlugin",
			GRPCPlugin{Impl: &PingPongPlayer{}},
			ErrPluginNameNotFound,
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
			// Necessary to redefine in every test due to future setting of stdout by the go-plugin package
			id.Cmd = *pluginServerCmd(id.ProtocolVersion, basicHS)
			if tc.p != nil {
				assertErr(t, tc.m.RegisterPlugin(registeredPluginName, id, basicHS, tc.p), tc.err, tc.description)
			}
			_, err := tc.m.LoadPluginClient(tc.name)
			assertErr(t, err, tc.err, tc.description)
		})
	}
}

func assertErr(t *testing.T, err, cmpErr error, desc string) {
	if err != nil && !errors.Is(err, cmpErr) {
		t.Errorf("'%s' failed: %v", desc, err)
	}
}

// Used to run the plugin server
func pluginServerCmd(version uint, auth PluginHandshake) *exec.Cmd {
	cs := []string{"-test.run=TestPluginServer", "--"}
	cs = append(cs, auth.Key, auth.Value, strconv.Itoa(int(version)))
	cmd := exec.Command(os.Args[0], cs...)

	env := []string{
		"RUN_PLUGIN_SERVER=1",
	}
	cmd.Env = append(env, os.Environ()...)
	return cmd
}

// This is not a real test. This is the plugin server that will be initialized by the tests
func TestPluginServer(*testing.T) {
	if os.Getenv("RUN_PLUGIN_SERVER") != "1" {
		return
	}

	defer os.Exit(0)

	args := os.Args
	for len(args) > 0 {
		if args[0] == "--" {
			args = args[1:]
			break
		}

		args = args[1:]
	}

	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "No command\n")
		os.Exit(2)
	}

	testGRPCPluginMap := map[string]plugin.Plugin{
		"test": &GRPCPlugin{Impl: &PingPongPlayer{}},
	}

	key := args[0]
	value := args[1]
	v, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	testHandshake := plugin.HandshakeConfig{
		ProtocolVersion:  uint(v),
		MagicCookieKey:   key,
		MagicCookieValue: value,
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: testHandshake,
		Plugins:         testGRPCPluginMap,
		GRPCServer:      plugin.DefaultGRPCServer,
	})

	// Shouldn't reach here but make sure we exit anyway
	os.Exit(0)
}
