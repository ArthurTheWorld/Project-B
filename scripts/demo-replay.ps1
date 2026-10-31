<#
.SYNOPSIS
    Prova a idempotência: apaga o checkpoint e reprocessa todos os eventos.
.EXAMPLE
    .\scripts\demo-replay.ps1
#>
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Contagem-Postgres {
    (docker compose exec -T postgres psql -U loja -d loja -t -A -c "select count(*) from loja.pedidos;").Trim()
}

function Contagem-Delta {
    $saida = docker compose exec -T spark /opt/spark/bin/spark-sql `
        --packages io.delta:delta-spark_2.12:3.2.0 `
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
        -f /opt/sql/verificacao.sql 2>&1
    ($saida | Select-String -Pattern "^\d+$" | Select-Object -First 1).Line
}

Write-Host "=== 1. Parando o gerador para estabilizar a contagem ===" -ForegroundColor Cyan
docker compose stop generator | Out-Null
Start-Sleep -Seconds 15

$antes = Contagem-Delta
Write-Host "Delta antes do replay: $antes linhas"

Write-Host "`n=== 2. Apagando checkpoint e reprocessando desde o início ===" -ForegroundColor Cyan
docker compose stop spark | Out-Null
docker compose run --rm --no-deps spark bash -c "rm -rf /data/checkpoints/pedidos"
docker compose up -d spark | Out-Null
Start-Sleep -Seconds 75

$depois = Contagem-Delta
$origem = Contagem-Postgres

Write-Host ""
Write-Host "Origem (Postgres): $origem"
Write-Host "Delta antes:       $antes"
Write-Host "Delta depois:      $depois"
Write-Host ""

if ($depois -eq $origem) {
    Write-Host "IDEMPOTENTE: reprocessar tudo não duplicou nenhuma linha." -ForegroundColor Green
} else {
    Write-Host "DIVERGIU: origem=$origem, delta=$depois" -ForegroundColor Red
}

docker compose up -d generator | Out-Null
