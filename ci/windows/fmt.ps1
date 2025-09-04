# Windows PowerShell equivalent of ci/fmt
$ErrorActionPreference = "Stop"

Write-Host "Running cargo fmt..."
cargo fmt --all -- --check

if ($LASTEXITCODE -ne 0) {
    Write-Host "Code formatting check failed. Run 'cargo fmt' to fix."
    exit $LASTEXITCODE
}

Write-Host "Cargo fmt passed!"