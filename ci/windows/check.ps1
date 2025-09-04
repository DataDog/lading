# Windows PowerShell equivalent of ci/check
$ErrorActionPreference = "Stop"

Write-Host "Running cargo check..."
# Check with default features but exclude Unix-specific prometheus features  
cargo check --locked --no-default-features

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Cargo check passed!"