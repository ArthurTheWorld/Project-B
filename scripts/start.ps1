<#
.SYNOPSIS
    Sobe todo o stack CDC e mostra o status.
.EXAMPLE
    .\scripts\start.ps1
#>
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker não encontrado. Abra o Docker Desktop e tente de novo." -ForegroundColor Red
    exit 1
}

Write-Host "Subindo o stack (primeira vez baixa jars do Delta/Kafka)..." -ForegroundColor Cyan
docker compose up -d --build

Write-Host "Aguardando serviços..." -ForegroundColor Cyan
Start-Sleep -Seconds 25
docker compose ps

Write-Host ""
Write-Host "Postgres (OLTP) ..... localhost:5435  (loja/loja)" -ForegroundColor Green
Write-Host "Kafka ............... localhost:9092" -ForegroundColor Green
Write-Host "Kafka Connect ....... http://localhost:8083" -ForegroundColor Green
Write-Host ""
Write-Host "Para provar idempotência: .\scripts\demo-replay.ps1" -ForegroundColor Yellow
Write-Host "Para provar evolução:     .\scripts\demo-evolucao.ps1" -ForegroundColor Yellow
