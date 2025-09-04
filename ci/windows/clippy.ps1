# Windows PowerShell equivalent of ci/clippy
$ErrorActionPreference = "Stop"

Write-Host "Running cargo clippy..."
cargo clippy --locked --no-default-features --all-targets -- -D warnings

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Cargo clippy passed!"