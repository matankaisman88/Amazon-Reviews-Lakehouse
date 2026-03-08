"""
Standalone script to run the pipeline and print progress to stdout.
Used by the dashboard so the pipeline runs in a subprocess and doesn't block Streamlit.
"""
import os
import sys

# Ensure project root is on path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    from src.utils.pipeline_orchestrator import run_refresh

    for line in run_refresh(target_date):
        print(line, flush=True)
