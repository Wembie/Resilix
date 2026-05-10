$ErrorActionPreference = "Stop"

Write-Host "SDK unit tests"
Set-Location "$PSScriptRoot\..\sdk\go"
go test ./...

Write-Host "Cross-module tests"
Set-Location "$PSScriptRoot\..\tests\go"
go test ./...
