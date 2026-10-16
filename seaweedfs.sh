#!/bin/sh

# This is the entrypoint script for the SeaweedFS container.
set -e

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${WEED_S3_ACCESS_KEY:?WEED_S3_ACCESS_KEY is required}"
: "${WEED_S3_SECRET_KEY:?WEED_S3_SECRET_KEY is required}"

eval "$(
python3 <<'PY'
import os
import shlex
from urllib.parse import urlparse, parse_qs, unquote

url = urlparse(os.environ["DATABASE_URL"])
query = parse_qs(url.query)

values = {
    "WEED_DB_HOST": url.hostname or "",
    "WEED_DB_PORT": str(url.port or 5432),
    "WEED_DB_USER": unquote(url.username or ""),
    "WEED_DB_PASSWORD": unquote(url.password or ""),
    "WEED_DB_NAME": url.path.lstrip("/"),
    "WEED_DB_SSLMODE": query.get("sslmode", ["require"])[0],
}

for key, value in values.items():
    print(f"export {key}={shlex.quote(value)}")
PY
)"

mkdir -p /etc/seaweedfs

envsubst < /filer.toml.template > /etc/seaweedfs/filer.toml

envsubst < /s3.json.template > /etc/seaweedfs/s3.json

exec weed server \
  -ip=0.0.0.0 \
  -dir=/data \
  -filer=true \
  -filer.port=8888 \
  -s3=true \
  -s3.port=8333 \
  -s3.config=/etc/seaweedfs/s3.json