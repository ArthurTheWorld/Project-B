<#
.SYNOPSIS
    Prova a evolução de schema: adiciona coluna no OLTP e verifica no Delta.
.EXAMPLE
    .\scripts\demo-evolucao.ps1
#>
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "=== 1. Adicionando coluna cupom na origem ===" -ForegroundColor Cyan
docker compose exec -T postgres psql -U loja -d loja -c `
    "alter table loja.pedidos add column cupom text; update loja.pedidos set cupom = 'BLACK50' where pedido_id % 7 = 0;"

Write-Host "`n=== 2. Aguardando o Spark processar os eventos ===" -ForegroundColor Cyan
Start-Sleep -Seconds 35

Write-Host "`n=== 3. Delta depois da evolução ===" -ForegroundColor Cyan
docker compose exec -T spark /opt/spark/bin/spark-sql `
    --packages io.delta:delta-spark_2.12:3.2.0 `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    -f /opt/sql/evolucao.sql
