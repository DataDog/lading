# Windows PowerShell equivalent of ci/test
$ErrorActionPreference = "Stop"

# Check if cargo-nextest is installed
if (-not (Get-Command cargo-nextest -ErrorAction SilentlyContinue)) {
    Write-Host "cargo-nextest is not installed. Installing it now..."
    cargo install cargo-nextest
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to install cargo-nextest"
        exit $LASTEXITCODE
    }
}

Write-Host "Running cargo nextest..."
# Run tests without Linux-specific features
cargo nextest run --features default

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Tests passed!"