#!/usr/bin/env bash
set -euo pipefail

# Pico-Banana-400K data bootstrap script
# - Downloads JSONL + manifests (via Python downloader, no curl/wget)
# - Downloads Open Images tarballs (via aws s3 cp)
# - Extracts full contents (no sampling)
# - Runs URL→local path mapping for SFT (and, if present, preference/multi-turn)

# Config via env vars (can be overridden when invoking):
#   DATA_DIR                Base data directory (default: openimages)
#   DOWNLOAD_EDITED_COUNT   Number of edited images to download per manifest (0=all, >0=that many) (requires aria2c or Python fallback)
#   AWS_ENDPOINT_URL        S3 endpoint (default: https://s3.amazonaws.com)

DATA_DIR=${DATA_DIR:-openimages}
DOWNLOAD_EDITED_COUNT=${DOWNLOAD_EDITED_COUNT:-0}  # 0 means download all
AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-https://s3.amazonaws.com}

JSONL_DIR="$DATA_DIR/jsonl"
MANIFEST_DIR="$DATA_DIR/manifest"
SOURCE_DIR="$DATA_DIR/source"
EDITED_DIR="$DATA_DIR/edited"

# Local bin directory for user-installed tools (no sudo needed)
LOCAL_BIN="$HOME/.local/bin"
mkdir -p "$LOCAL_BIN"
export PATH="$LOCAL_BIN:$PATH"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN=python
else
  echo "Error: neither python3 nor python found in PATH." >&2
  exit 1
fi

# Function to install aria2c locally (no sudo needed)
install_aria2c_local() {
  if command -v aria2c >/dev/null 2>&1; then
    return 0
  fi
  
  echo "📦 aria2c not found. Attempting to install locally (no sudo needed)..."
  
  # Try pip install first (easiest method)
  if $PYTHON_BIN -m pip --version >/dev/null 2>&1; then
    echo "  Installing aria2p (Python wrapper for aria2)..."
    $PYTHON_BIN -m pip install --user aria2p 2>/dev/null || true
  fi
  
  # Try conda if available
  if command -v conda >/dev/null 2>&1; then
    echo "  Trying conda install aria2..."
    conda install -y -c conda-forge aria2 2>/dev/null || true
  fi
  
  # If still not available, download static binary
  if ! command -v aria2c >/dev/null 2>&1; then
    echo "  Downloading aria2 static binary..."
    local aria2_version="1.36.0"
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    
    if [ "$os" = "linux" ] && [ "$arch" = "x86_64" ]; then
      local url="https://github.com/aria2/aria2/releases/download/release-${aria2_version}/aria2-${aria2_version}-linux-gnu-64bit-build1.tar.bz2"
      local tmpdir=$(mktemp -d)
      
      if command -v wget >/dev/null 2>&1; then
        wget -q -O "$tmpdir/aria2.tar.bz2" "$url" 2>/dev/null || return 1
      elif command -v curl >/dev/null 2>&1; then
        curl -sL -o "$tmpdir/aria2.tar.bz2" "$url" 2>/dev/null || return 1
      else
        echo "  Cannot download aria2: wget/curl not available"
        return 1
      fi
      
      tar -xjf "$tmpdir/aria2.tar.bz2" -C "$tmpdir" 2>/dev/null
      cp "$tmpdir/aria2-${aria2_version}-linux-gnu-64bit-build1/aria2c" "$LOCAL_BIN/" 2>/dev/null
      chmod +x "$LOCAL_BIN/aria2c"
      rm -rf "$tmpdir"
      
      if command -v aria2c >/dev/null 2>&1; then
        echo "  ✅ aria2c installed successfully to $LOCAL_BIN"
      fi
    fi
  fi
  
  if ! command -v aria2c >/dev/null 2>&1; then
    echo "  ⚠️  Could not install aria2c automatically. Will use fallback method."
    echo "  Tip: You can manually install with: pip install --user aria2p"
    return 1
  fi
}

# Function to install 7z locally (no sudo needed)
install_7z_local() {
  if command -v 7z >/dev/null 2>&1 || command -v 7za >/dev/null 2>&1; then
    return 0
  fi
  
  echo "📦 7z not found. Attempting to install locally (no sudo needed)..."
  
  # Try conda if available (fastest method)
  if command -v conda >/dev/null 2>&1; then
    echo "  Trying conda install p7zip..."
    conda install -y -c conda-forge p7zip 2>/dev/null || true
  fi
  
  # Try pip install (py7zr - pure Python implementation)
  if ! command -v 7z >/dev/null 2>&1 && ! command -v 7za >/dev/null 2>&1; then
    if $PYTHON_BIN -m pip --version >/dev/null 2>&1; then
      echo "  Installing py7zr (Python 7z library)..."
      $PYTHON_BIN -m pip install --user py7zr 2>/dev/null || true
    fi
  fi
  
  # Download static binary for Linux x86_64
  if ! command -v 7z >/dev/null 2>&1 && ! command -v 7za >/dev/null 2>&1; then
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    
    if [ "$os" = "linux" ] && [ "$arch" = "x86_64" ]; then
      echo "  Downloading 7z static binary..."
      local url="https://www.7-zip.org/a/7z2301-linux-x64.tar.xz"
      local tmpdir=$(mktemp -d)
      
      if command -v wget >/dev/null 2>&1; then
        wget -q -O "$tmpdir/7z.tar.xz" "$url" 2>/dev/null || return 1
      elif command -v curl >/dev/null 2>&1; then
        curl -sL -o "$tmpdir/7z.tar.xz" "$url" 2>/dev/null || return 1
      else
        echo "  Cannot download 7z: wget/curl not available"
        return 1
      fi
      
      tar -xJf "$tmpdir/7z.tar.xz" -C "$LOCAL_BIN" 2>/dev/null
      chmod +x "$LOCAL_BIN/7zz" 2>/dev/null
      ln -sf "$LOCAL_BIN/7zz" "$LOCAL_BIN/7z" 2>/dev/null
      rm -rf "$tmpdir"
      
      if command -v 7z >/dev/null 2>&1; then
        echo "  ✅ 7z installed successfully to $LOCAL_BIN"
      fi
    fi
  fi
  
  if ! command -v 7z >/dev/null 2>&1 && ! command -v 7za >/dev/null 2>&1; then
    echo "  ⚠️  Could not install 7z automatically. Will try pigz or standard tar."
    echo "  Tip: You can manually install with: conda install -c conda-forge p7zip"
    return 1
  fi
}

# Function to install pigz locally (no sudo needed)
install_pigz_local() {
  if command -v pigz >/dev/null 2>&1; then
    return 0
  fi
  
  echo "📦 pigz not found. Attempting to install locally (no sudo needed)..."
  
  # Try conda if available
  if command -v conda >/dev/null 2>&1; then
    echo "  Trying conda install pigz..."
    conda install -y -c conda-forge pigz 2>/dev/null || true
  fi
  
  # Download static binary for Linux x86_64
  if ! command -v pigz >/dev/null 2>&1; then
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    
    if [ "$os" = "linux" ] && [ "$arch" = "x86_64" ]; then
      echo "  Downloading pigz static binary..."
      local url="https://zlib.net/pigz/pigz-2.8.tar.gz"
      local tmpdir=$(mktemp -d)
      
      if command -v wget >/dev/null 2>&1; then
        wget -q -O "$tmpdir/pigz.tar.gz" "$url" 2>/dev/null || return 1
      elif command -v curl >/dev/null 2>&1; then
        curl -sL -o "$tmpdir/pigz.tar.gz" "$url" 2>/dev/null || return 1
      else
        echo "  Cannot download pigz: wget/curl not available"
        return 1
      fi
      
      tar -xzf "$tmpdir/pigz.tar.gz" -C "$tmpdir" 2>/dev/null
      cd "$tmpdir/pigz-2.8" && make 2>/dev/null && cp pigz "$LOCAL_BIN/" 2>/dev/null
      chmod +x "$LOCAL_BIN/pigz" 2>/dev/null
      cd - >/dev/null
      rm -rf "$tmpdir"
      
      if command -v pigz >/dev/null 2>&1; then
        echo "  ✅ pigz installed successfully to $LOCAL_BIN"
      fi
    fi
  fi
  
  if ! command -v pigz >/dev/null 2>&1; then
    echo "  ⚠️  Could not install pigz automatically. Will use standard tar."
    echo "  Tip: You can manually install with: conda install -c conda-forge pigz"
    return 1
  fi
}

# Try to install tools locally
echo "🔧 Checking for performance tools..."
install_aria2c_local || true
install_7z_local || true
install_pigz_local || true
echo ""

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

# Multi-threaded Python downloader for batch downloads
py_download_batch() {
  local url_file="$1"
  local outdir="$2"
  local split_name="$3"
  local max_workers="${4:-32}"
  
  "$PYTHON_BIN" - "$url_file" "$outdir" "$split_name" "$max_workers" <<'PY'
import sys
import os
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

url_file = Path(sys.argv[1])
outdir = Path(sys.argv[2])
split_name = sys.argv[3]
max_workers = int(sys.argv[4])

outdir.mkdir(parents=True, exist_ok=True)

# Read all URLs
with url_file.open() as f:
    urls = [line.strip() for line in f if line.strip()]

total = len(urls)
completed = 0
skipped = 0
failed = 0
lock = Lock()
start_time = time.time()

def download_one(url):
    global completed, skipped, failed
    try:
        # Extract filename from URL (remove query params)
        fname = url.split('/')[-1].split('?')[0]
        dest = outdir / fname
        
        # Skip if already exists
        if dest.exists():
            with lock:
                skipped += 1
                return f"SKIP: {fname}"
        
        # Download with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urlopen(req, timeout=60) as resp:
                    with dest.open("wb") as out:
                        out.write(resp.read())
                
                with lock:
                    completed += 1
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed - skipped) / rate if rate > 0 else 0
                    
                    # Progress every 100 files or every 10 seconds
                    if completed % 100 == 0 or int(elapsed) % 10 == 0:
                        print(f"  [{split_name}] Progress: {completed}/{total} downloaded, "
                              f"{skipped} skipped, {failed} failed | "
                              f"{rate:.1f} files/s | ETA: {eta/60:.1f}min", 
                              flush=True)
                
                return f"OK: {fname}"
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    with lock:
                        failed += 1
                    return f"FAIL: {fname} - {str(e)}"
    
    except Exception as e:
        with lock:
            failed += 1
        return f"ERROR: {url} - {str(e)}"

print(f"  [{split_name}] Starting download of {total} files with {max_workers} threads...", flush=True)

# Download with thread pool
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(download_one, url): url for url in urls}
    
    for future in as_completed(futures):
        result = future.result()
        # Only print errors
        if result.startswith("FAIL") or result.startswith("ERROR"):
            print(f"  [{split_name}] {result}", flush=True)

elapsed = time.time() - start_time
print(f"  [{split_name}] ✅ Complete! Downloaded: {completed}, Skipped: {skipped}, "
      f"Failed: {failed}, Total: {total} | Time: {elapsed/60:.1f}min", flush=True)

sys.exit(0 if failed == 0 else 1)
PY
}

build_missing_list() {
  local subset_file="$1"
  local outdir="$2"
  local output_file="$3"

  $PYTHON_BIN - "$subset_file" "$outdir" "$output_file" <<'PY'
import sys
from pathlib import Path

subset = Path(sys.argv[1])
outdir = Path(sys.argv[2])
output = Path(sys.argv[3])

if not subset.exists():
    if output.exists():
        output.unlink()
    sys.exit(0)

missing = []
with subset.open() as fh:
    for line in fh:
        url = line.strip()
        if not url:
            continue
        fname = url.split("/")[-1].split("?")[0]
        dest = outdir / fname
        if not dest.is_file():
            missing.append(url)

if missing:
    output.write_text("\n".join(missing) + "\n")
else:
    if output.exists():
        output.unlink()
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

# Function to download with aria2c if available (supports multi-threaded download)
download_with_aria2() {
  local url="$1"
  local dest="$2"
  local filename=$(basename "$dest")
  
  if [ -f "$dest" ]; then
    echo "  $filename already downloaded, skipping..."
    return 0
  fi
  
  echo "  Downloading $filename (this may take a while)..."
  
  # Try aria2c first (supports multi-connection download)
  if command -v aria2c >/dev/null 2>&1; then
    echo "  Using aria2c for faster multi-threaded download (16 connections)..."
    # aria2c with progress display, summary interval, and console log level
    aria2c -x 16 -s 16 -k 1M \
      --allow-overwrite=true \
      --summary-interval=5 \
      --console-log-level=notice \
      -d "$(dirname "$dest")" \
      -o "$filename" \
      "$url"
  else
    # Fallback to aws s3 cp
    echo "  Using aws s3 cp (install aria2c for faster downloads)..."
    aws s3 --no-sign-request --endpoint-url "$AWS_ENDPOINT_URL" cp "$url" "$dest" --no-progress 2>&1 | \
      grep -v "Completed" || aws s3 --no-sign-request --endpoint-url "$AWS_ENDPOINT_URL" cp "$url" "$dest"
  fi
}

# Download both files (can be done in parallel if needed)
download_with_aria2 "s3://open-images-dataset/tar/train_0.tar.gz" "$SOURCE_DIR/train_0.tar.gz" &
PID_TRAIN0=$!

download_with_aria2 "s3://open-images-dataset/tar/train_1.tar.gz" "$SOURCE_DIR/train_1.tar.gz" &
PID_TRAIN1=$!

# Wait for both downloads to complete
echo "  Waiting for parallel downloads to complete..."
wait $PID_TRAIN0
wait $PID_TRAIN1
echo "  All downloads completed!"

echo "[5/6] Extracting Open Images (full extract; large and slow) ..."

# Function to extract with pigz if available (parallel gzip decompression)
extract_tarball() {igure 3. Qualitative results for object-removal on RORD validation dataset [75]. Best viewed zoomed in. Our model uses a single NFE
and is able to successfully remove not only the object but also its shadow. Additional results are provided in the appendices.
  local tarball="$1"
  local extract_dir="$2"
  local marker="$3"
  local name=$(basename "$tarball" .tar.gz)
  
  # Check extraction marker file (more reliable than directory check)
  if [ -f "$marker" ]; then
    # Verify that extraction actually completed by checking for image files
    local jpg_count=$(find "$extract_dir" -maxdepth 3 -name "*.jpg" -type f 2>/dev/null | head -100 | wc -l)
    if [ "$jpg_count" -gt 0 ]; then
      echo "  $name already extracted (found $jpg_count+ images), skipping..."
      return 0
    else
      echo "  ⚠️  Marker exists but no images found. Re-extracting..."
      rm -f "$marker"
    fi
  fi
  
  echo "→ Full extraction of $(basename "$tarball")"
  
  # Get CPU count for optimal thread usage
  local threads=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
  
  # Priority 1: Use pigz (parallel gzip) if available - most stable
  if command -v pigz >/dev/null 2>&1; then
    echo "  Using pigz with $threads threads for faster parallel decompression..."
    
    # Check if pv (pipe viewer) is available for progress bar
    if command -v pv >/dev/null 2>&1; then
      local filesize=$(stat -f%z "$tarball" 2>/dev/null || stat -c%s "$tarball" 2>/dev/null)
      echo "  Progress: extracting $(basename "$tarball") ($(numfmt --to=iec-i --suffix=B $filesize 2>/dev/null || echo "size: $filesize bytes"))"
      pv -p -t -e -r -b "$tarball" | pigz -p $threads -dc | tar -xf - -C "$extract_dir"
    else
      # No pv, just extract directly
      echo "  Extracting (this will take a while)..."
      pigz -p $threads -dc "$tarball" | tar -xf - -C "$extract_dir"
    fi
    
  # Priority 2: Standard tar (slowest, but most compatible)
  else
    echo "  Using standard tar (install pigz for faster extraction)..."
    
    # Check if pv is available for standard tar as well
    if command -v pv >/dev/null 2>&1; then
      local filesize=$(stat -f%z "$tarball" 2>/dev/null || stat -c%s "$tarball" 2>/dev/null)
      echo "  Progress: extracting $(basename "$tarball")"
      pv -p -t -e -r -b "$tarball" | tar -xzf - -C "$extract_dir"
    else
      echo "  Extracting (this will take a while)..."
      tar -xzf "$tarball" -C "$extract_dir"
    fi
  fi
  
  # Verify extraction succeeded
  local jpg_count=$(find "$extract_dir" -maxdepth 3 -name "*.jpg" -type f 2>/dev/null | head -10 | wc -l)
  if [ "$jpg_count" -gt 0 ]; then
    touch "$marker"
    echo "  ✅ Extraction of $name completed successfully!"
  else
    echo "  ⚠️  Warning: Extraction completed but no .jpg files found!"
    echo "     Archive may be corrupted or have unexpected structure."
  fi
}

# Extract both tarballs in parallel
extract_tarball "$SOURCE_DIR/train_0.tar.gz" "$SOURCE_DIR" "$SOURCE_DIR/.train_0_extracted" &
PID_EXTRACT0=$!

extract_tarball "$SOURCE_DIR/train_1.tar.gz" "$SOURCE_DIR" "$SOURCE_DIR/.train_1_extracted" &
PID_EXTRACT1=$!

# Wait for both extractions to complete
echo "  Extracting both archives in parallel..."
wait $PID_EXTRACT0
wait $PID_EXTRACT1
echo "  All extractions completed!"

echo "[Optional] Downloading edited images (0=all, N>0=first N per split): $DOWNLOAD_EDITED_COUNT"
download_subset() {
  local manifest="$1"
  local outdir="$2"
  local n="${3:-0}"
  local split_name="$4"
  
  [ -f "$manifest" ] || {
    echo "  [$split_name] manifest not found: $manifest" >&2
    return 0
  }
  
  mkdir -p "$outdir"
  local subset="$outdir/_subset_urls.txt"
  local retry_file="$outdir/_retry_urls.txt"
  local failed_file="$outdir/_failed_urls.txt"
  rm -f "$subset" "$retry_file"
  # Always start with a clean failed log so reruns only report fresh misses
  rm -f "$failed_file"
  
  # If n=0, download all; otherwise download first n
  if [ "$n" -eq 0 ]; then
    echo "  [$split_name] Downloading ALL edited images from $(basename "$manifest")..."
    cp "$manifest" "$subset"
  else
    echo "  [$split_name] Downloading first $n edited images from $(basename "$manifest")..."
    head -n "$n" "$manifest" > "$subset"
  fi
  
  local url_count=$(wc -l < "$subset")
  echo "  [$split_name] Total URLs to download: $url_count"
  
  # Use Python multi-threaded downloader (works everywhere, no external dependencies)
  echo "  [$split_name] Using Python multi-threaded downloader (32 threads)..."
  local parallel_status=0
  if ! py_download_batch "$subset" "$outdir" "$split_name" 32; then
    parallel_status=$?
  fi
  
  if [ $parallel_status -ne 0 ]; then
    echo "  [$split_name] ⚠️  Parallel downloader reported failures. Checking for missing files..."
    build_missing_list "$subset" "$outdir" "$retry_file"
    
    local retry_count=0
    if [ -f "$retry_file" ]; then
      retry_count=$(wc -l < "$retry_file")
    fi
    
    if [ "$retry_count" -gt 0 ]; then
      echo "  [$split_name] Retrying $retry_count files sequentially..."
      while IFS= read -r url || [ -n "$url" ]; do
        [ -n "$url" ] || continue
        local clean_url="${url%%\?*}"
        local filename=$(basename "$clean_url")
        local dest="$outdir/$filename"
        
        if [ -f "$dest" ]; then
          continue
        fi
        
        if ! py_download "$url" "$dest"; then
          echo "    [$split_name] retry failed for $filename"
        fi
      done < "$retry_file"
      
      build_missing_list "$subset" "$outdir" "$retry_file"
      local remaining=0
      if [ -f "$retry_file" ]; then
        remaining=$(wc -l < "$retry_file")
      fi
      
      if [ "$remaining" -gt 0 ]; then
        mv "$retry_file" "$failed_file"
        echo "  [$split_name] ❌ Still missing $remaining files after retry. URLs saved to $failed_file"
      else
        echo "  [$split_name] ✅ Sequential retry recovered all missing files."
        parallel_status=0
      fi
    else
      echo "  [$split_name] Parallel downloader errored, but all files appear to be present."
      parallel_status=0
    fi
  fi
  
  # Count actual downloaded files
  local downloaded_count=$(find "$outdir" -maxdepth 1 -type f \( -name "*.png" -o -name "*.jpg" -o -name "*.jpeg" \) 2>/dev/null | wc -l)
  echo "  [$split_name] Files in directory: $downloaded_count"
  
  rm -f "$subset" "$retry_file"
  
  if [ $parallel_status -ne 0 ]; then
    return 1
  fi
  
  rm -f "$failed_file"
  return 0
}

# Download all three splits in parallel for maximum speed
echo "  Starting parallel downloads for all splits (sft, preference, multi_turn)..."
download_subset "$MANIFEST_DIR/sft_manifest.txt" "$EDITED_DIR/sft" "$DOWNLOAD_EDITED_COUNT" "SFT" &
PID_SFT=$!

download_subset "$MANIFEST_DIR/preference_manifest.txt" "$EDITED_DIR/preference" "$DOWNLOAD_EDITED_COUNT" "PREF" &
PID_PREF=$!

download_subset "$MANIFEST_DIR/multi_turn_manifest.txt" "$EDITED_DIR/multi_turn" "$DOWNLOAD_EDITED_COUNT" "MT" &
PID_MT=$!

# Wait for all three downloads to complete
echo "  Waiting for parallel downloads to complete..."
wait $PID_SFT 2>/dev/null || true
wait $PID_PREF 2>/dev/null || true
wait $PID_MT 2>/dev/null || true
echo "  🎉 All edited image downloads completed!"

echo "[6/6] Mapping source URLs → local image paths..."

# Verify that source images exist before mapping
if [ ! -d "$SOURCE_DIR" ] || [ $(find "$SOURCE_DIR" -name "*.jpg" -type f 2>/dev/null | head -1 | wc -l) -eq 0 ]; then
  echo "⚠️  Warning: No .jpg files found in $SOURCE_DIR"
  echo "   This might indicate extraction is incomplete or failed."
  echo "   Please check the extraction markers and try re-running extraction."
fi

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

echo ""
echo "✅ Done. Key outputs:"
echo "  - $JSONL_DIR/sft_with_local_source_image_path.jsonl"
echo "  - Extracted sources under: $SOURCE_DIR"
echo "  - Edited images under: $EDITED_DIR/{sft,preference,multi_turn}"
echo ""
echo "Tip: set DOWNLOAD_EDITED_COUNT=N to download only first N edited images, or 0 for all (default)."
