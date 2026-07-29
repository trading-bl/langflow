#!/bin/sh
set -eu

# Kubernetes injects LANGFLOW_PORT=tcp://... when service links are enabled and
# the Service is named "langflow". Langflow expects this variable to be an
# integer, so replace an injected/non-numeric value with the deployment port.
case "${LANGFLOW_PORT:-}" in
    ''|*[!0-9]*)
        export LANGFLOW_PORT=8989
        ;;
esac

exec "$@"
