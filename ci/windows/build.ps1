# Windows PowerShell script to build release binaries
$ErrorActionPreference = "Stop"

Write-Host "Building release binary for Windows..."
cargo build --locked --release

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed"
    exit $LASTEXITCODE
}

Write-Host "Build completed successfully!"