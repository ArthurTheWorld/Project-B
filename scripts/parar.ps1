<#
.SYNOPSIS
    Para o stack preservando os dados (volumes).
#>
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

docker compose down
Write-Host "Stack parado. Volumes preservados." -ForegroundColor Yellow
