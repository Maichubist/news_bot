# Run the test suite (Windows equivalent of `make test`).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }
& $python -m pytest
exit $LASTEXITCODE
