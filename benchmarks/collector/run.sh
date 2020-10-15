#!/bin/sh

echo $GRAFANA_CONFIG | base64 -d > /etc/agent/agent.yaml

/bin/agent "$@"
