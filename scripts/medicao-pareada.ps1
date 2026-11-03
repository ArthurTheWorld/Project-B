param([switch]$Drain)
$ErrorActionPreference = 'Continue'
$out = 'C:\Users\luiza\Documents\deepseek-harness\default-workspace\cdc-lakehouse\tmp_medicao.log'
function Log($m) { $m | Out-File -FilePath $out -Append -Encoding utf8 }

if ($Drain) {
    docker stop cdc-lakehouse-generator-1 | Out-Null
    Log "generator parado em $(Get-Date -Format o)"
    Start-Sleep -Seconds 60
    Log "drenagem: 60s de espera concluidos"
}

docker exec cdc-lakehouse-postgres-1 psql -U loja -d loja -t -A -c "select count(*) from loja.pedidos;" 2>$null | ForEach-Object { Log "origem=$_" }

docker cp sql\conta_delta.sql cdc-lakehouse-spark-1:/tmp/conta.sql | Out-Null
docker exec cdc-lakehouse-spark-1 /opt/spark/bin/spark-sql --master local[1] --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog -f /tmp/conta.sql 2>$null |
    Where-Object { $_ -match '^[0-9]+$' } | ForEach-Object { Log "delta=$_" }

Log '--- fim ---'
Get-Content $out
