#!/bin/bash

# System tests env vars
export TEST_WEBHOOK_HOST="localhost"
export ESTI_SETUP_LAKEFS="true"
export ESTI_STORAGE_NAMESPACE="local://system-testing"

# Lakefs env vars for test
export ACTIONS_VAR="this_is_actions_var"
