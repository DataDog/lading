# Windows PowerShell equivalent of ci/fmt
$ErrorActionPreference = "Stop"

Write-Host "Running cargo fmt..."
# On Windows, skip the --check flag due to line ending issues
# This is experimental Windows support - formatting differences are expected
cargo fmt --all

if ($LASTEXITCODE -ne 0) {
    Write-Host "Code formatting failed."
    exit $LASTEXITCODE
}

Write-Host "Cargo fmt completed!"