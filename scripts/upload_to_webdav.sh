#!/usr/bin/env bash
# Upload weather parquet files to Hetzner WebDAV storage
#
# Usage:
#   export WEBDAV_USER=your_username
#   export WEBDAV_PASS=your_password
#   ./scripts/upload_to_webdav.sh [REGION]

set -e

REGION=${1:-tampere}
DATE=$(date -u +%Y%m%d)

LOCAL_DIR="/tmp/weather_parquet/${REGION}"

# Check credentials
if [ -z "$WEBDAV_USER" ] || [ -z "$WEBDAV_PASS" ]; then
    echo "Error: WEBDAV_USER and WEBDAV_PASS must be set"
    echo "  export WEBDAV_USER=your_username"
    echo "  export WEBDAV_PASS=your_password"
    exit 1
fi

# Check if local files exist
if [ ! -d "$LOCAL_DIR" ] || [ -z "$(ls -A "$LOCAL_DIR" 2>/dev/null)" ]; then
    echo "No files to upload in $LOCAL_DIR"
    echo "Run ./scripts/fetch_and_ingest.sh first"
    exit 1
fi

echo "=== WebDAV Upload ==="
echo "Source: $LOCAL_DIR"
echo "Dest: storagebox://${WEBDAV_USER}/weather/${REGION}/"
echo ""

echo "Creating directory structure..."
curl -X MKCOL "https://${WEBDAV_USER}.your-storagebox.de/weather/${REGION}" \
  -u "${WEBDAV_USER}:${WEBDAV_PASS}" \
  -w "%{http_code}\n" -s -o /dev/null || true

# Upload files
echo "Uploading parquet files..."
for FILE in "$LOCAL_DIR"/*.parquet; do
    BASENAME=$(basename "$FILE")
    echo "  Uploading $BASENAME..."
    curl -T "$FILE" \
      "https://${WEBDAV_USER}.your-storagebox.de/weather/${REGION}/${BASENAME}" \
      -u "${WEBDAV_USER}:${WEBDAV_PASS}" \
      -w "    HTTP %{http_code}\n" -s -o /dev/null
done

echo ""
echo "Upload complete!"
echo ""
echo "To read remotely with DuckDB:"
echo ""
cat << 'EOF'
INSTALL webdavfs FROM community;
LOAD webdavfs;
INSTALL h3 FROM community;
LOAD h3;

CREATE SECRET hetzner (
    TYPE WEBDAV,
    USERNAME 'your_user',
    PASSWORD 'your_pass',
    SCOPE 'storagebox://your_user'
);
EOF
echo ""
echo "SELECT * FROM read_parquet('storagebox://${WEBDAV_USER}/weather/${REGION}/*.parquet');"
