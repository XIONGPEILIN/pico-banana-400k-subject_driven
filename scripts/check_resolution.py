"""
分辨率检查工具：
- 支持单张图片检查；
- 支持扫描 temp_verify 同款数据目录下的日志，批量检查 before 图；
- 汇总是否满足 16 对齐。
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

import torch.multiprocessing as mp
from PIL import Image
from tqdm import tqdm

DEFAULT_WORK_DIR = "openimages/pico_sam_output_ALL_20251206_032609"


def check_divisible_by_16(image_path: Path) -> dict:
    """返回图片宽、高以及是否同时被16整除。"""
    img = Image.open(image_path).convert("RGB")
    w, h = img.size
    return {
        "path": str(image_path),
        "width": w,
        "height": h,
        "divisible_by_16": (w % 16 == 0) and (h % 16 == 0),
    }


def check_single(image_path: Path):
    info = check_divisible_by_16(image_path)
    print(
        f"{info['path']} => 宽:{info['width']} 高:{info['height']} "
        f"能否被16整除:{info['divisible_by_16']}"
    )


def parse_log_and_check(log_path: str) -> Optional[dict]:
    """解析单个日志并返回分辨率信息。"""
    try:
        with open(log_path, "r") as f:
            data = json.load(f)
        img_before = data.get("original_item", {}).get("local_input_image")
        if not img_before:
            return None
        img_path = Path(img_before)
        if not img_path.exists():
            return None
        return check_divisible_by_16(img_path)
    except Exception:
        return None


def check_dataset(work_dir: Path, num_workers: int = 1):
    log_files = [
        str(p)
        for p in work_dir.glob("item_*/*_log.json")
        if p.name.startswith("item_") and p.name.endswith("_log.json")
    ]
    top_logs = [
        str(p)
        for p in work_dir.glob("item_*_log.json")
        if p.name.startswith("item_") and p.name.endswith("_log.json")
    ]
    # 去重后合并
    seen = set(log_files)
    for p in top_logs:
        if p not in seen:
            log_files.append(p)
    log_files = sorted(log_files)

    if not log_files:
        print(f"未找到日志文件，目录: {work_dir}")
        return

    results: List[dict] = []
    if num_workers > 1:
        with mp.Pool(processes=num_workers) as pool:
            for res in tqdm(
                pool.imap_unordered(parse_log_and_check, log_files, chunksize=64),
                total=len(log_files),
                desc="解析日志",
            ):
                if res:
                    results.append(res)
    else:
        for log_path in tqdm(log_files, desc="解析日志"):
            res = parse_log_and_check(log_path)
            if res:
                results.append(res)

    if not results:
        print("没有有效的 before 图像记录。")
        return

    total = len(results)
    ok = sum(1 for r in results if r["divisible_by_16"])
    not_ok = total - ok

    print(f"检查完成，目录: {work_dir}")
    print(f"总计: {total}, 可被16整除: {ok}, 不可: {not_ok}")

    if not_ok > 0:
        print("不可整除的样本：")
        for r in results:
            if not r["divisible_by_16"]:
                print(
                    f"- {r['path']} (宽:{r['width']} 高:{r['height']})"
                )


def parse_args():
    parser = argparse.ArgumentParser(
        description="检查图片分辨率是否可被16整除。"
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="图片路径或数据目录；为空时默认扫描 temp_verify 的 WORK_DIR。",
    )
    parser.add_argument(
        "--dataset",
        action="store_true",
        help="将输入视为数据目录，扫描日志批量检查 before 图。",
    )
    parser.add_argument(
        "--work-dir",
        default=DEFAULT_WORK_DIR,
        help="当未提供 path 时，默认扫描的工作目录（与 temp_verify 对齐）。",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (mp.cpu_count() or 1) // 2),
        help="进程数，多核加速日志解析与图片读取。",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.dataset:
        target_dir = Path(args.path) if args.path else Path(args.work_dir)
        check_dataset(target_dir, num_workers=args.workers)
        return

    if args.path:
        image_path = Path(args.path)
        if not image_path.exists():
            print(f"文件不存在: {image_path}")
            sys.exit(1)
        check_single(image_path)
    else:
        # 未指定 path 且未使用 --dataset：默认扫描工作目录
        check_dataset(Path(args.work_dir), num_workers=args.workers)


if __name__ == "__main__":
    main()
