$ErrorActionPreference = "Stop"

Write-Host "Syncing Go workspace..."
go work sync

Write-Host "Running Go unit tests..."
Set-Location "$PSScriptRoot\..\sdk\go"
go test ./...
