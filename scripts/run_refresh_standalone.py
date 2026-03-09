"""
Standalone script to run the pipeline and print progress to stdout.
Used by the dashboard so the pipeline runs in a subprocess and doesn't block Streamlit.

Usage: python scripts/run_refresh_standalone.py [--category=Category] [--max-rows=N] [--overwrite-raw] [--skip-optimize]
  --category=Gift_Cards  (default: Gift_Cards)
  --max-rows=N           limit rows per category when fetching (omit for config default; 0 = unlimited)
  --overwrite-raw        re-fetch and overwrite existing raw files
  --skip-optimize        skip Gold OPTIMIZE/Z-ORDER (faster runs)
"""
import os
import sys

# Ensure project root is on path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

if __name__ == "__main__":
    category = None
    max_rows = None
    overwrite_raw = False
    skip_optimize = False
    for arg in sys.argv[1:]:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
        elif arg.startswith("--max-rows="):
            val = arg.split("=", 1)[1]
            # 0 = unlimited (pass through); null/empty = use config; else int
            if val in ("null", ""):
                max_rows = None
            else:
                max_rows = 0 if val == "0" else int(val)
        elif arg == "--overwrite-raw":
            overwrite_raw = True
        elif arg == "--skip-optimize":
            skip_optimize = True
    from src.utils.pipeline_orchestrator import run_refresh

    for line in run_refresh(category=category, max_rows=max_rows, overwrite_raw=overwrite_raw, skip_optimize=skip_optimize):
        print(line, flush=True)
