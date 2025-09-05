# Windows PowerShell equivalent of ci/check
$ErrorActionPreference = "Stop"

Write-Host "Running cargo check..."
cargo check --locked --features default

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Cargo check passed!"