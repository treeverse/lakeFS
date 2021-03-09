package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Physical Address: {{ .PhysicalAddress }}
Checksum: {{ .Checksum }}
`

const fsRecursiveTemplate = `Files: {{.Count}}
Total Size: {{.Bytes}} bytes
Human Total Size: {{.Bytes|human_bytes}}
`

var fsStatCmd = &cobra.Command{
	Use:   "stat <path uri>",
	Short: "view object metadata",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		stat, err := client.StatObject(cmd.Context(), pathURI.Repository, pathURI.Ref, *pathURI.Path)
		if err != nil {
			DieErr(err)
		}

		Write(fsStatTemplate, stat)
	},
}

const fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 6 }}    {{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}    {{ $val.Path|yellow }}
{{ end -}}
`

var fsListCmd = &cobra.Command{
	Use:   "ls <path uri>",
	Short: "list entries under a given tree",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		var from string
		for {
			results, more, err := client.ListObjects(cmd.Context(), pathURI.Repository, pathURI.Ref, *pathURI.Path, from, -1)
			if err != nil {
				DieErr(err)
			}
			if len(results) > 0 {
				Write(fsLsTemplate, results)
			}
			if !swag.BoolValue(more.HasMore) {
				break
			}
			from = more.NextOffset
		}
	},
}

var fsCatCmd = &cobra.Command{
	Use:   "cat <path uri>",
	Short: "dump content of object to stdout",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		_, err := client.GetObject(cmd.Context(), pathURI.Repository, pathURI.Ref, *pathURI.Path, os.Stdout)
		if err != nil {
			DieErr(err)
		}
	},
}

func upload(ctx context.Context, client api.Client, sourcePathname string, destURI *uri.URI) (*models.ObjectStats, error) {
	fp := OpenByPath(sourcePathname)
	defer func() {
		_ = fp.Close()
	}()

	// read
	return client.UploadObject(ctx, destURI.Repository, destURI.Ref, *destURI.Path, fp)
}

var fsUploadCmd = &cobra.Command{
	Use:   "upload <path uri>",
	Short: "upload a local file to the specified URI",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		source, _ := cmd.Flags().GetString("source")
		recursive, _ := cmd.Flags().GetBool("recursive")
		if !recursive {
			stat, err := upload(cmd.Context(), client, source, pathURI)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
			return
		}
		// copy recursively
		var totals struct {
			Bytes int64
			Count int64
		}
		err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("traverse %s: %w", path, err)
			}
			if info.IsDir() {
				return nil
			}
			relPath := strings.TrimPrefix(path, source)
			uri := *pathURI
			p := filepath.Join(*uri.Path, relPath)
			uri.Path = &p
			stat, err := upload(cmd.Context(), client, path, &uri)
			if err != nil {
				return fmt.Errorf("upload %s: %w", path, err)
			}
			totals.Bytes += swag.Int64Value(stat.SizeBytes)
			totals.Count++
			return nil
		})
		if err != nil {
			DieErr(err)
		}
		Write(fsRecursiveTemplate, totals)
	},
}

var fsStageCmd = &cobra.Command{
	Use:    "stage <path uri>",
	Short:  "stages a reference to an existing object, to be managed in lakeFS",
	Hidden: true,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		size, _ := cmd.Flags().GetInt64("size")
		location, _ := cmd.Flags().GetString("location")
		checksum, _ := cmd.Flags().GetString("checksum")
		meta, metaErr := getKV(cmd, "meta")

		obj := &models.ObjectStageCreation{
			Checksum:        swag.String(checksum),
			PhysicalAddress: swag.String(location),
			SizeBytes:       swag.Int64(size),
		}
		if metaErr == nil {
			obj.Metadata = meta
		}

		stat, err := client.StageObject(cmd.Context(), pathURI.Repository, pathURI.Ref, *pathURI.Path, obj)
		if err != nil {
			DieErr(err)
		}

		Write(fsStatTemplate, stat)
	},
}

var fsRmCmd = &cobra.Command{
	Use:   "rm <path uri>",
	Short: "delete object",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		err := client.DeleteObject(cmd.Context(), pathURI.Repository, pathURI.Ref, *pathURI.Path)
		if err != nil {
			DieErr(err)
		}
	},
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "view and manipulate objects",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
	fsCmd.AddCommand(fsListCmd)
	fsCmd.AddCommand(fsCatCmd)
	fsCmd.AddCommand(fsUploadCmd)
	fsCmd.AddCommand(fsStageCmd)
	fsCmd.AddCommand(fsRmCmd)

	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	fsUploadCmd.Flags().BoolP("recursive", "r", false, "recursively copy all files under local source")
	_ = fsUploadCmd.MarkFlagRequired("source")

	fsStageCmd.Flags().String("location", "", "fully qualified storage location (i.e. \"s3://bucket/path/to/object\")")
	fsStageCmd.Flags().Int64("size", 0, "Object size in bytes")
	fsStageCmd.Flags().String("checksum", "", "Object MD5 checksum as a hexadecimal string")
	fsStageCmd.Flags().StringSlice("meta", []string{}, "key value pairs in the form of key=value")

	_ = fsStageCmd.MarkFlagRequired("location")
	_ = fsStageCmd.MarkFlagRequired("size")
	_ = fsStageCmd.MarkFlagRequired("checksum")
}
