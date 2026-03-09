"""
Input size estimation for dynamic Spark configuration.
Estimates total bytes under a path (local/mounted) to drive shuffle partitions and AQE.
"""

import os
from pathlib import Path
from typing import List, Union


def estimate_input_size_bytes(paths: Union[str, Path, List[Union[str, Path]]]) -> int:
    """
    Estimate total size in bytes of files under the given path(s).
    Works for local/mounted paths (e.g. /data/raw/amazon, /data/bronze/...).
    Recursively sums file sizes; skips directories.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]
    total = 0
    for p in paths:
        path = Path(p)
        if not path.exists():
            continue
        if path.is_file():
            try:
                total += path.stat().st_size
            except OSError:
                pass
        else:
            try:
                for root, _, files in os.walk(path):
                    for f in files:
                        fp = Path(root) / f
                        try:
                            total += fp.stat().st_size
                        except OSError:
                            pass
            except OSError:
                pass
    return total


def compute_shuffle_partitions(
    input_size_bytes: int,
    target_partition_size_bytes: int = 134_217_728,  # 128MB
    min_partitions: int = 8,
    max_partitions: int = 400,
) -> int:
    """
    Compute shuffle partitions from input size.
    Rule: ~128–200MB per partition. Min 8, max 200–400.
    """
    if input_size_bytes <= 0:
        return min_partitions
    partitions = max(1, input_size_bytes // target_partition_size_bytes)
    return max(min_partitions, min(partitions, max_partitions))
