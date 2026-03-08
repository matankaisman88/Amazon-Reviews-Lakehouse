"""
Standalone script to run the pipeline and print progress to stdout.
Used by the dashboard so the pipeline runs in a subprocess and doesn't block Streamlit.

Usage: python scripts/run_refresh_standalone.py [--category=Category]
  --category=Gift_Cards  (default: Gift_Cards)
"""
import os
import sys

# Ensure project root is on path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

if __name__ == "__main__":
    category = None
    for arg in sys.argv[1:]:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
            break
    from src.utils.pipeline_orchestrator import run_refresh

    for line in run_refresh(category=category):
        print(line, flush=True)
