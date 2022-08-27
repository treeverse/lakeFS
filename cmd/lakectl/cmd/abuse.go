package cmd

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/testutil/stress"
)

var abuseCmd = &cobra.Command{
	Use:    "abuse <sub command>",
	Short:  "Abuse a running lakeFS instance. See sub commands for more info.",
	Hidden: true,
}

func readLines(path string) (lines []string, err error) {
	reader := OpenByPath(path)
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("%w, and while closing %s", err, closeErr)
			}
		}
	}()
	scanner := bufio.NewScanner(reader)
	lines = make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

var abuseRandomReadsCmd = &cobra.Command{
	Use:    "random-read <source ref uri>",
	Short:  "Read keys from a file and generate random reads from the source ref for those keys.",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		amount := MustInt(cmd.Flags().GetInt("amount"))
		parallelism := MustInt(cmd.Flags().GetInt("parallelism"))
		fromFile := MustString(cmd.Flags().GetString("from-file"))

		Fmt("Source ref: %s\n", u.String())
		// read the input file
		keys, err := readLines(fromFile)
		if err != nil {
			DieErr(err)
		}
		Fmt("read a total of %d keys from key file\n", len(keys))

		generator := stress.NewGenerator("read", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		rand.Seed(time.Now().Unix())
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				//nolint:gosec
				add(keys[rand.Intn(len(keys))])
			}
		})

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()
				resp, err := client.StatObjectWithResponse(ctx, u.Repository, u.Ref, &api.StatObjectParams{
					Path: work,
				})
				if err == nil && resp.StatusCode() != http.StatusOK {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

var abuseLinkSameObjectCmd = &cobra.Command{
	Use:    "link-same-object <source ref uri>",
	Short:  "Link the same object in parallel.",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		amount := MustInt(cmd.Flags().GetInt("amount"))
		parallelism := MustInt(cmd.Flags().GetInt("parallelism"))
		key := MustString(cmd.Flags().GetString("key"))

		Fmt("Source ref: %s\n", u.String())
		Fmt("Object key: %s\n", key)

		generator := stress.NewGenerator("get-and-link", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// setup generator to use the key
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(key)
			}
		})

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()

				getResponse, err := client.GetPhysicalAddressWithResponse(ctx, u.Repository, u.Ref, &api.GetPhysicalAddressParams{Path: work})
				if err == nil && getResponse.JSON200 == nil {
					err = helpers.ResponseAsError(getResponse)
				}
				if err != nil {
					output <- stress.Result{
						Error: err,
						Took:  time.Since(start),
					}
					continue
				}

				stagingLocation := getResponse.JSON200
				linkResponse, err := client.LinkPhysicalAddressWithResponse(ctx, u.Repository, u.Ref,
					&api.LinkPhysicalAddressParams{
						Path: work,
					},
					api.LinkPhysicalAddressJSONRequestBody{
						Checksum: "00695c7307b0480c7b6bdc873cf05c15",
						Staging: api.StagingLocation{
							PhysicalAddress: stagingLocation.PhysicalAddress,
							Token:           stagingLocation.Token,
						},
						UserMetadata: nil,
					})
				if err == nil && linkResponse.JSON200 == nil {
					err = helpers.ResponseAsError(linkResponse)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

var abuseRandomWritesCmd = &cobra.Command{
	Use:    "random-write <source branch uri>",
	Short:  "Generate random writes to the source branch",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source branch", args[0])
		amount := MustInt(cmd.Flags().GetInt("amount"))
		parallelism := MustInt(cmd.Flags().GetInt("parallelism"))
		prefix := MustString(cmd.Flags().GetString("prefix"))

		Fmt("Source branch: %s\n", u.String())
		generator := stress.NewGenerator("stage object", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		rand.Seed(time.Now().Unix())
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(fmt.Sprintf("%sfile-%d", prefix, i))
			}
		})

		client := getClient()
		resp, err := client.GetRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		repo := resp.JSON200
		storagePrefix := repo.StorageNamespace
		var size int64
		checksum := "00695c7307b0480c7b6bdc873cf05c15"
		addr := storagePrefix + "/random-write"
		creationInfo := api.ObjectStageCreation{
			Checksum:        checksum,
			PhysicalAddress: addr,
			SizeBytes:       size,
		}

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()
				resp, err := client.StageObjectWithResponse(ctx, u.Repository, u.Ref, &api.StageObjectParams{Path: work},
					api.StageObjectJSONRequestBody(creationInfo))
				if err == nil && resp.StatusCode() != http.StatusOK {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

var abuseCommitCmd = &cobra.Command{
	Use:    "commit <source ref uri>",
	Short:  "Commits to the source ref repeatedly",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		amount := MustInt(cmd.Flags().GetInt("amount"))
		gapDuration := MustDuration(cmd.Flags().GetDuration("gap"))

		Fmt("Source branch: %s\n", u.String())
		generator := stress.NewGenerator("commit", 1, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		rand.Seed(time.Now().Unix())
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(strconv.Itoa(i + 1))
			}
		})

		// generate randomly selected keys as input
		client := getClient()
		resp, err := client.GetRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for work := range input {
				start := time.Now()
				resp, err := client.CommitWithResponse(ctx, u.Repository, u.Ref, &api.CommitParams{},
					api.CommitJSONRequestBody(api.CommitCreation{Message: work}))
				if err == nil && resp.StatusCode() != http.StatusOK {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(gapDuration):
				}
			}
		})
	},
}

var abuseCreateBranchesCmd = &cobra.Command{
	Use:    "create-branches <source ref uri>",
	Short:  "Create a lot of branches very quickly.",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		cleanOnly := MustBool(cmd.Flags().GetBool("clean-only"))
		branchPrefix := MustString(cmd.Flags().GetString("branch-prefix"))
		amount := MustInt(cmd.Flags().GetInt("amount"))
		parallelism := MustInt(cmd.Flags().GetInt("parallelism"))

		client := getClient()

		Fmt("Source ref: %s\n", u.String())
		deleteGen := stress.NewGenerator("delete branch", parallelism)

		const paginationAmount = 1000
		deleteGen.Setup(func(add stress.GeneratorAddFn) {
			currentOffset := api.PaginationAfter(branchPrefix)
			amount := api.PaginationAmount(paginationAmount)
			for {
				resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &api.ListBranchesParams{
					After:  &currentOffset,
					Amount: &amount,
				})
				DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

				for _, ref := range resp.JSON200.Results {
					if !strings.HasPrefix(ref.Id, branchPrefix) {
						return
					}
					add(ref.Id) // this branch should be deleted!
				}
				pagination := resp.JSON200.Pagination
				if !pagination.HasMore {
					return
				}
				currentOffset = api.PaginationAfter(pagination.NextOffset)
			}
		})

		// wait for deletes to end
		deleteGen.Run(func(input chan string, output chan stress.Result) {
			for branch := range input {
				start := time.Now()
				_, err := client.DeleteBranchWithResponse(cmd.Context(), u.Repository, branch)
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})

		if cleanOnly {
			return // done.
		}

		// start creating branches
		generator := stress.NewGenerator("create branch", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate create branch requests
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(fmt.Sprintf("%s-%d", branchPrefix, i))
			}
		})

		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			for branch := range input {
				start := time.Now()
				resp, err := client.CreateBranchWithResponse(
					ctx, u.Repository, api.CreateBranchJSONRequestBody{
						Name:   branch,
						Source: u.Ref,
					})
				if err == nil && resp.StatusCode() != http.StatusCreated {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

var abuseListCmd = &cobra.Command{
	Use:    "list <source ref uri>",
	Short:  "List from the source ref",
	Hidden: false,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("source ref", args[0])
		amount := MustInt(cmd.Flags().GetInt("amount"))
		parallelism := MustInt(cmd.Flags().GetInt("parallelism"))
		prefix := MustString(cmd.Flags().GetString("prefix"))

		generator := stress.NewGenerator("list", parallelism, stress.WithSignalHandlersFor(os.Interrupt, syscall.SIGTERM))

		// generate randomly selected keys as input
		rand.Seed(time.Now().Unix())
		generator.Setup(func(add stress.GeneratorAddFn) {
			for i := 0; i < amount; i++ {
				add(strconv.Itoa(i + 1))
			}
		})

		listPrefix := api.PaginationPrefix(prefix)
		// execute the things!
		generator.Run(func(input chan string, output chan stress.Result) {
			ctx := cmd.Context()
			client := getClient()
			for range input {
				start := time.Now()
				resp, err := client.ListObjectsWithResponse(ctx, u.Repository, u.Ref, &api.ListObjectsParams{
					Prefix: &listPrefix,
				})
				if err == nil && resp.StatusCode() != http.StatusOK {
					err = helpers.ResponseAsError(resp)
				}
				output <- stress.Result{
					Error: err,
					Took:  time.Since(start),
				}
			}
		})
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(abuseCmd)

	abuseCmd.AddCommand(abuseCreateBranchesCmd)

	const (
		defaultAmount      = 1000000
		defaultParallelism = 100
	)

	abuseCreateBranchesCmd.Flags().String("branch-prefix", "abuse-", "prefix to create branches under")
	abuseCreateBranchesCmd.Flags().Bool("clean-only", false, "only clean up past runs")
	abuseCreateBranchesCmd.Flags().Int("amount", defaultAmount, "amount of things to do")
	abuseCreateBranchesCmd.Flags().Int("parallelism", defaultParallelism, "amount of things to do in parallel")

	abuseCmd.AddCommand(abuseRandomReadsCmd)
	abuseRandomReadsCmd.Flags().String("from-file", "", "read keys from this file (\"-\" for stdin)")
	abuseRandomReadsCmd.Flags().Int("amount", defaultAmount, "amount of reads to do")
	abuseRandomReadsCmd.Flags().Int("parallelism", defaultParallelism, "amount of reads to do in parallel")

	abuseCmd.AddCommand(abuseRandomWritesCmd)
	abuseRandomWritesCmd.Flags().String("prefix", "abuse/", "prefix to create paths under")
	abuseRandomWritesCmd.Flags().Int("amount", defaultAmount, "amount of writes to do")
	abuseRandomWritesCmd.Flags().Int("parallelism", defaultParallelism, "amount of writes to do in parallel")

	abuseCmd.AddCommand(abuseCommitCmd)
	abuseCommitCmd.Flags().Int("amount", defaultParallelism, "amount of commits to do")
	abuseCommitCmd.Flags().Duration("gap", 2*time.Second, "duration to wait between commits")

	abuseCmd.AddCommand(abuseListCmd)
	abuseListCmd.Flags().String("prefix", "abuse/", "prefix to list under")
	abuseListCmd.Flags().Int("amount", defaultAmount, "amount of lists to do")
	abuseListCmd.Flags().Int("parallelism", defaultParallelism, "amount of lists to do in parallel")

	abuseCmd.AddCommand(abuseLinkSameObjectCmd)
	abuseLinkSameObjectCmd.Flags().Int("amount", defaultAmount, "amount of link object to do")
	abuseLinkSameObjectCmd.Flags().Int("parallelism", defaultParallelism, "amount of link object to do in parallel")
	abuseLinkSameObjectCmd.Flags().String("key", "linked-object", "key used for the test")
}
