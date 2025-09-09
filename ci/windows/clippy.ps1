# Windows PowerShell equivalent of ci/clippy
$ErrorActionPreference = "Stop"

Write-Host "Running cargo clippy..."
cargo clippy --locked --features default --all-targets -- -D warnings

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Cargo clippy passed!"