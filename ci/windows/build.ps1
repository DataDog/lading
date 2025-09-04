# Windows PowerShell script to build release binaries
$ErrorActionPreference = "Stop"

Write-Host "Building release binary for Windows..."
# Build with default features only (excludes Linux-specific features)
cargo build --locked --release --no-default-features

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed"
    exit $LASTEXITCODE
}

Write-Host "Build completed successfully!"
Write-Host "Binary location: target/release/lading.exe"