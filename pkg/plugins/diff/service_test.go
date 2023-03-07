package tablediff

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/pkg/config"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

func TestService_RunDiff(t *testing.T) {
	testCases := []struct {
		register    bool
		diffFailure bool
		description string
		expectedErr error
	}{
		{
			register:    true,
			description: "successful run",
			expectedErr: nil,
		},
		{
			register:    false,
			description: "failure - no client loaded",
			expectedErr: ErrNotFound,
		},
		{
			register:    true,
			diffFailure: true,
			description: "failure - internal diff failed",
			expectedErr: ErrDiffFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			service := NewMockService()
			if tc.register {
				service.registerDiffClient(diffType, internal.HCPluginProperties{})
			}
			ctx = ContextWithError(ctx, tc.expectedErr)
			_, err := service.RunDiff(ctx, diffType, Params{})
			if err != nil && !errors.Is(err, tc.expectedErr) {
				t.Errorf("'%s' failed: %s", tc.description, err)
			}

		})
	}
}

func Test_registerPlugins(t *testing.T) {
	pluginName := "p"
	customPluginPath := "a/plugin/path"
	customPluginVersion := 9
	type args struct {
		service     *Service
		diffProps   map[string]config.DiffProps
		pluginProps map[string]config.PluginProps
		pluginsPath string
	}
	testCases := []struct {
		description string
		diffType    string
		pluginName  string
		args        args
		expectedErr error
	}{
		{
			description: "register delta diff plugin - default path and version - success",
			diffType:    "delta",
			args: args{
				service: NewMockService(),
				diffProps: map[string]config.DiffProps{
					"delta": {
						PluginName: pluginName,
					},
				},
				pluginProps: nil,
				pluginsPath: "defaultPath",
			},
		},
		{
			description: "register delta diff plugin - custom path and version - success",
			diffType:    "delta",
			pluginName:  pluginName,
			args: args{
				service: NewMockService(),
				diffProps: map[string]config.DiffProps{
					"delta": {
						PluginName: pluginName,
					},
				},
				pluginProps: map[string]config.PluginProps{
					pluginName: {
						Path:    customPluginPath,
						Version: &customPluginVersion,
					},
				},
				pluginsPath: "defaultPath",
			},
		},
		{
			description: "register unknown diff plugin - default path - failure",
			diffType:    "unknown",
			args: args{
				service: NewMockService(),
				diffProps: map[string]config.DiffProps{
					"unknown": {
						PluginName: pluginName,
					},
				},
				pluginProps: nil,
				pluginsPath: "defaultPath",
			},
			expectedErr: ErrNotFound,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			registerPlugins(tc.args.service, tc.args.diffProps, tc.args.pluginProps, tc.args.pluginsPath)
			client, _, err := tc.args.service.pluginHandler.LoadPluginClient(tc.diffType)
			if err != nil && !errors.Is(err, tc.expectedErr) {
				t.Errorf("'%s' failed: %s", tc.description, err)
			}
			if client != nil {
				diffs, err := client.Diff(context.Background(), Params{})
				if err != nil && !errors.Is(err, tc.expectedErr) {
					t.Errorf("'%s' failed: %s", tc.description, err)
				}
				pluginDetails := diffs.Diffs[0].OperationContent
				tcPath := tc.args.pluginsPath + "/" + tc.args.diffProps[tc.diffType].PluginName
				if tc.args.pluginProps[tc.pluginName].Path != "" {
					tcPath = tc.args.pluginProps[tc.pluginName].Path
				}
				if pluginDetails[PluginPath] != tcPath {
					t.Errorf("'%s' failed: incorrect plugin path. got '%s' instead of  '%s'",
						tc.description,
						pluginDetails[PluginPath],
						tcPath)
				}
				tcVersion := tc.args.pluginProps[tc.pluginName].Version
				if tcVersion != nil && pluginDetails[PluginVersion] != strconv.Itoa(*tcVersion) {
					t.Errorf("'%s' failed: incorrect plugin version. got '%s' instead of  '%s'",
						tc.description,
						pluginDetails[PluginVersion],
						strconv.Itoa(*tcVersion))
				}
			}
		})
	}
}
