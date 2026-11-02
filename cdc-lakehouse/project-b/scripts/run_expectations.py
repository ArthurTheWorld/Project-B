# Run Great Expectations suites for Projeto B
# Usage: python -m great_expectations suite validate expectations/pedidos_expectations.py
#        python -m great_expectations suite validate expectations/mercado_expectations.py

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTATIONS_DIR = PROJECT_ROOT / "expectations"

def run_suite(suite_name: str) -> int:
    """Execute a Great Expectations suite validation."""
    suite_file = EXPECTATIONS_DIR / f"{suite_name}.py"
    if not suite_file.is_file():
        print(f"⚠ Suite file not found: {suite_file}")
        return 1
    result = subprocess.run(
        [sys.executable, "-m", "great_expectations", "suite", "validate", str(suite_file)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    return result.returncode

def main() -> None:
    suites = ["pedidos_expectations", "mercado_expectations"]
    exit_codes = [run_suite(s) for s in suites]
    sys.exit(1 if any(exit_codes) else 0)

if __name__ == "__main__":
    main()