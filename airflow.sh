#!/bin/bash

if [ "$1" = "" ]; then
    echo "Usage: $0 <variable_name>"
    exit 1
fi

if [ "$MWAA_ENVIRONMENT" = "" ]; then
    echo "MWAA_ENVIRONMENT is not set"
    exit 1
fi

CLI_JSON=$(aws mwaa create-cli-token --name "$MWAA_ENVIRONMENT")
CLI_TOKEN=$(echo "$CLI_JSON" | jq -r '.CliToken')
WEB_SERVER_HOSTNAME=$(echo "$CLI_JSON" | jq -r '.WebServerHostname')

RESPONSE=$(curl -s --request POST "https://$WEB_SERVER_HOSTNAME/aws_mwaa/cli/" \
    --header "Authorization: Bearer $CLI_TOKEN" \
    --header "Content-Type: text/plain" \
    --data-raw "variables get $1")

STDERR=$(echo "$RESPONSE" | jq -r '.stderr // empty')
if [ "$STDERR" != "" ]; then
    echo "$RESPONSE" | jq -r '.stderr' | base64 -d >&2
    exit 1
fi

echo "$RESPONSE" | jq -r '.stdout' | base64 -d | tr -d '\n'