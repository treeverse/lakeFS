#!/bin/bash -eu
compile_go_fuzzer github.com/treeverse/lakefs/pkg/gateway/http FuzzParseRange fuzz_parse_range
