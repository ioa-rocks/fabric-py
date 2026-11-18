"""Run the SDK against two disposable Fabric nodes from a source checkout.

Usage: python3 tests/run_daemon_tests.py /path/to/fabric
The Go test overlay leaves that checkout unchanged.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


sdk = Path(__file__).resolve().parents[1]
fabric = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="sdk-overlay-") as temporary:
    overlay = Path(temporary) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(fabric / "astral/daemon/python_sdk_test.go"):
        str(sdk / "tests/astral_sdk_integration_test.go"),
    }}))
    result = subprocess.run([
        "go", "test", "-overlay", str(overlay), "-run", "^TestPythonSDKCredentials$",
        "-count=1", "-timeout=90s", "-v", "./astral/daemon",
    ], cwd=fabric, env={**os.environ, "FABRIC_PY_SDK": str(sdk)})
    raise SystemExit(result.returncode)
