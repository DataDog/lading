# Windows PowerShell script to build release binaries
$ErrorActionPreference = "Stop"

Write-Host "Building release binary for Windows..."
# Build without Linux-specific features
cargo build --locked --release --exclude-features logrotate_fs

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed"
    exit $LASTEXITCODE
}

Write-Host "Build completed successfully!"
Write-Host "Binary location: target/release/lading.exe"