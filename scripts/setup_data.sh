#!/usr/bin/env bash
set -euo pipefail

# Pico-Banana-400K data bootstrap script
# - Downloads JSONL + manifests (via Python downloader, no curl/wget)
# - Downloads Open Images tarballs (via aws s3 cp)
# - Extracts full contents (no sampling)
# - Runs URL→local path mapping for SFT (and, if present, preference/multi-turn)

# Config via env vars (can be overridden when invoking):
#   DATA_DIR                Base data directory (default: openimages)
#   DOWNLOAD_EDITED_COUNT   If >0, download this many edited images from each manifest (requires aria2c or Python fallback)
#   AWS_ENDPOINT_URL        S3 endpoint (default: https://s3.amazonaws.com)

DATA_DIR=${DATA_DIR:-openimages}
DOWNLOAD_EDITED_COUNT=${DOWNLOAD_EDITED_COUNT:-0}
AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-https://s3.amazonaws.com}

JSONL_DIR="$DATA_DIR/jsonl"
MANIFEST_DIR="$DATA_DIR/manifest"
SOURCE_DIR="$DATA_DIR/source"
EDITED_DIR="$DATA_DIR/edited"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN=python
else
  echo "Error: neither python3 nor python found in PATH." >&2
  exit 1
fi

py_download() {
  local url="$1"
  local dest="$2"
  if [ -f "$dest" ]; then
    echo "  already downloaded: $dest"
    return 0
  fi
  "$PYTHON_BIN" - "$url" "$dest" <<'PY'
import sys
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import shutil

url = sys.argv[1]
dest = Path(sys.argv[2])
dest.parent.mkdir(parents=True, exist_ok=True)

req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
try:
    with urlopen(req) as resp, dest.open("wb") as out:
        shutil.copyfileobj(resp, out)
except HTTPError as e:
    raise SystemExit(f"HTTP error {e.code} for {url}: {e.reason}")
except URLError as e:
    raise SystemExit(f"Failed to download {url}: {e.reason}")
PY
}

SFT_JSONL_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/jsonl/sft.jsonl"
PREF_JSONL_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/jsonl/preference.jsonl"
MT_JSONL_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/jsonl/multi-turn.jsonl"

SFT_MANIFEST_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/manifest/sft_manifest.txt"
PREF_MANIFEST_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/manifest/preference_manifest.txt"
MT_MANIFEST_URL="https://ml-site.cdn-apple.com/datasets/pico-banana-300k/nb/manifest/multi_turn_manifest.txt"

CSV_URL="https://storage.googleapis.com/openimages/2018_04/train/train-images-boxable-with-rotation.csv"
TRAIN0_URL="https://s3.amazonaws.com/open-images-dataset/tar/train_0.tar.gz"
TRAIN1_URL="https://s3.amazonaws.com/open-images-dataset/tar/train_1.tar.gz"

mkdir -p "$JSONL_DIR" "$MANIFEST_DIR" "$SOURCE_DIR" "$EDITED_DIR/sft" "$EDITED_DIR/preference" "$EDITED_DIR/multi_turn"

# Resolve repository root to call the mapper reliably regardless of CWD
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "[1/6] Downloading JSONL files (Python)..."
py_download "$SFT_JSONL_URL" "$JSONL_DIR/sft.jsonl"
py_download "$PREF_JSONL_URL" "$JSONL_DIR/preference.jsonl" || true
py_download "$MT_JSONL_URL" "$JSONL_DIR/multi-turn.jsonl" || true

echo "[2/6] Downloading manifest files (Python)..."
py_download "$SFT_MANIFEST_URL" "$MANIFEST_DIR/sft_manifest.txt"
py_download "$PREF_MANIFEST_URL" "$MANIFEST_DIR/preference_manifest.txt" || true
py_download "$MT_MANIFEST_URL" "$MANIFEST_DIR/multi_turn_manifest.txt" || true

echo "[3/6] Downloading Open Images metadata CSV (Python)..."
py_download "$CSV_URL" "$DATA_DIR/train-images-boxable-with-rotation.csv"

echo "[4/6] Downloading Open Images tarballs via aws s3 (this is large)..."
if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI not found. Install with: pip install awscli (or your package manager)." >&2
  exit 1
fi
aws s3 --no-sign-request --endpoint-url "$AWS_ENDPOINT_URL" cp "s3://open-images-dataset/tar/train_0.tar.gz" "$SOURCE_DIR/train_0.tar.gz"
aws s3 --no-sign-request --endpoint-url "$AWS_ENDPOINT_URL" cp "s3://open-images-dataset/tar/train_1.tar.gz" "$SOURCE_DIR/train_1.tar.gz"

echo "[5/6] Extracting Open Images (full extract; large and slow) ..."
echo "→ Full extraction of train_0.tar.gz"
tar -xvzf "$SOURCE_DIR/train_0.tar.gz" -C "$SOURCE_DIR"
echo "→ Full extraction of train_1.tar.gz"
tar -xvzf "$SOURCE_DIR/train_1.tar.gz" -C "$SOURCE_DIR"

echo "[Optional] Downloading edited images (first N per split): $DOWNLOAD_EDITED_COUNT"
download_subset() {
  local manifest="$1"
  local outdir="$2"
  local n="${3:-0}"
  [ "$n" -gt 0 ] || return 0
  [ -f "$manifest" ] || {
    echo "  manifest not found: $manifest" >&2
    return 0
  }
  mkdir -p "$outdir"
  local subset="$outdir/_subset_urls.txt"
  head -n "$n" "$manifest" > "$subset"
  if command -v aria2c >/dev/null 2>&1; then
    aria2c -x16 -s16 -k1M -d "$outdir" -i "$subset"
  else
    # Fallback to Python loop (no curl/wget)
    while IFS= read -r url; do
      [ -n "$url" ] || continue
      fname=$(basename "${url%%\?*}")
      echo "Downloading $fname"
      if [ -f "$outdir/$fname" ]; then
        echo "  already exists, skipping"
        continue
      fi
      py_download "$url" "$outdir/$fname"
    done < "$subset"
  fi
  rm -f "$subset"
}

download_subset "$MANIFEST_DIR/sft_manifest.txt" "$EDITED_DIR/sft" "$DOWNLOAD_EDITED_COUNT" || true
download_subset "$MANIFEST_DIR/preference_manifest.txt" "$EDITED_DIR/preference" "$DOWNLOAD_EDITED_COUNT" || true
download_subset "$MANIFEST_DIR/multi_turn_manifest.txt" "$EDITED_DIR/multi_turn" "$DOWNLOAD_EDITED_COUNT" || true

echo "[6/6] Mapping source URLs → local image paths..."
$PYTHON_BIN "$REPO_ROOT/map_openimage_url_to_local.py" \
  --metadata_csv "$DATA_DIR/train-images-boxable-with-rotation.csv" \
  --jsonl_in "$JSONL_DIR/sft.jsonl" \
  --jsonl_out "$JSONL_DIR/sft_with_local_source_image_path.jsonl" \
  --image_root "$SOURCE_DIR"

# If preference / multi-turn JSONLs exist, map them too
if [ -f "$JSONL_DIR/preference.jsonl" ]; then
  $PYTHON_BIN "$REPO_ROOT/map_openimage_url_to_local.py" \
    --metadata_csv "$DATA_DIR/train-images-boxable-with-rotation.csv" \
    --jsonl_in "$JSONL_DIR/preference.jsonl" \
    --jsonl_out "$JSONL_DIR/preference_with_local_source_image_path.jsonl" \
    --image_root "$SOURCE_DIR"
fi

if [ -f "$JSONL_DIR/multi-turn.jsonl" ]; then
  $PYTHON_BIN "$REPO_ROOT/map_openimage_url_to_local.py" \
    --metadata_csv "$DATA_DIR/train-images-boxable-with-rotation.csv" \
    --jsonl_in "$JSONL_DIR/multi-turn.jsonl" \
    --jsonl_out "$JSONL_DIR/multi-turn_with_local_source_image_path.jsonl" \
    --image_root "$SOURCE_DIR" \
    --is_multi_turn
fi

echo "\n✅ Done. Key outputs:"
echo "  - $JSONL_DIR/sft_with_local_source_image_path.jsonl"
echo "  - Extracted sources under: $SOURCE_DIR"
echo "  - (Optional) Edited samples under: $EDITED_DIR/{sft,preference,multi_turn}"
echo "\nTip: set DOWNLOAD_EDITED_COUNT to fetch edited-image subsets if needed."
