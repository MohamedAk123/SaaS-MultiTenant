"""Compatibility entry point for the full six-part report."""
import runpy
from pathlib import Path
runpy.run_path(str(Path(__file__).with_name("build_full_report.py")), run_name="__main__")
