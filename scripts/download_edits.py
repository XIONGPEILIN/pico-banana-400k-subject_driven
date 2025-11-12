#!/usr/bin/env python3
"""Download edited images listed in a manifest using multithreading."""

import argparse
import concurrent.futures
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from tqdm.auto import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download edited images defined in a manifest file"
    )
    parser.add_argument("manifest", type=Path, help="Path to manifest txt file")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("openimages/edited/sft"),
        help="Directory to store downloaded images",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on number of URLs to download (0 = all)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Number of concurrent download threads",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Socket timeout for each request (seconds)",
    )
    return parser.parse_args()


def read_manifest(path: Path, limit: int) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")
    urls: list[str] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            urls.append(line)
            if limit and len(urls) >= limit:
                break
    if not urls:
        raise ValueError(f"No URLs parsed from manifest: {path}")
    return urls


def _relative_path(url: str) -> Path:
    path = urlsplit(url).path
    if not path:
        return Path("unknown")
    path = path.lstrip("/")
    marker = "/images/"
    if marker in path:
        idx = path.index(marker) + 1  # keep leading 'images/...'
        path = path[idx:]
    return Path(path)


def download_one(url: str, root: Path, timeout: int) -> tuple[str, str | None]:
    rel_path = _relative_path(url)
    target = root / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return (url, "exists")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=timeout) as resp, target.open("wb") as out:
            out.write(resp.read())
        return (url, "downloaded")
    except HTTPError as err:
        return (url, f"http {err.code}")
    except URLError as err:
        return (url, f"url {err.reason}")
    except Exception as err:  # noqa: B902
        return (url, f"error {err}")


def main() -> int:
    args = parse_args()
    urls = read_manifest(args.manifest, args.limit)
    args.output.mkdir(parents=True, exist_ok=True)

    total = len(urls)
    print(f"Downloading {total} files to {args.output} with {args.workers} workers")
    successes = 0
    skipped = 0
    failures: list[tuple[str, str | None]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {
            pool.submit(download_one, url, args.output, args.timeout): url for url in urls
        }
        for future in tqdm(
            concurrent.futures.as_completed(future_map),
            total=total,
            desc="Downloads",
            unit="img",
        ):
            url = future_map[future]
            try:
                _, status = future.result()
            except Exception as err:  # noqa: B902
                failures.append((url, f"exception {err}"))
                continue

            if status == "downloaded":
                successes += 1
            elif status == "exists":
                skipped += 1
            else:
                failures.append((url, status))

    print(f"✔ Completed: {successes}")
    print(f"↺ Skipped existing: {skipped}")
    print(f"✖ Failures: {len(failures)}")
    if failures:
        fail_log = args.output / "_failed.txt"
        with fail_log.open("w", encoding="utf-8") as fh:
            for url, reason in failures:
                fh.write(f"{reason}\t{url}\n")
        print(f"  Details saved to {fail_log}")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
