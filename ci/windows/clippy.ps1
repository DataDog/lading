# Windows PowerShell equivalent of ci/clippy
$ErrorActionPreference = "Stop"

Write-Host "Running cargo clippy..."
cargo clippy --all-features

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Cargo clippy passed!"