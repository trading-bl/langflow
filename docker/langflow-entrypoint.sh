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

# Build the PostgreSQL URL from Secret-backed Kubernetes environment variables
# when the deployment does not provide one explicitly. URL-encode credentials
# so punctuation in a generated password cannot corrupt the connection string.
if [ -z "${LANGFLOW_DATABASE_URL:-}" ] && [ -n "${POSTGRES_HOST:-}" ] && [ -n "${POSTGRES_PASSWORD:-}" ]; then
    export LANGFLOW_DATABASE_URL="$(
        python3.14 - <<'PY'
import os
from urllib.parse import quote

user = quote(os.environ.get("POSTGRES_USER", "langflow"), safe="")
password = quote(os.environ["POSTGRES_PASSWORD"], safe="")
host = os.environ["POSTGRES_HOST"]
port = os.environ.get("POSTGRES_PORT", "5432")
database = quote(os.environ.get("POSTGRES_DB", "langflow"), safe="")
print(f"postgresql://{user}:{password}@{host}:{port}/{database}")
PY
    )"
fi

exec "$@"
