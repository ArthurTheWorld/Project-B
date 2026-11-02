"""Consumo CDC -> Delta Lake com MERGE idempotente.

OpenLineage NAO e instrumentado aqui em Python: em Spark ele e um listener Scala
habilitado por config (`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`).
Ver o servico `spark` em docker-compose.yml: a instrumentacao e OPT-IN por
OPENLINEAGE_ENABLED (default desligado) e o job roda 100% sem ela.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from merge_sql import montar_merge

# Configuracao vinda do ambiente do container (docker-compose.yml, servico `spark`).
# Defaults iguais aos do compose para o job nao quebrar se a env faltar.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICO = os.getenv("TOPICO", "loja.loja.pedidos")
DELTA_PATH = os.getenv("DELTA_PATH", "/data/delta/pedidos")
CHECKPOINT = os.getenv("CHECKPOINT", "/data/checkpoints/pedidos")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")


def aplicar_lote(lote, batch_id: int) -> None:
    """Recebe o batch (DataFrame com a coluna `raw`) e aplica no Delta."""
    spark = lote.sparkSession

    brutos = [linha["raw"] for linha in lote.select("raw").collect() if linha["raw"]]
    if not brutos:
        return

    eventos = spark.read.json(spark.sparkContext.parallelize(brutos))

    # Instrumentacao: timestamps de evento e de ingestao
    agora = F.current_timestamp()
    evento_em = F.to_timestamp(F.col("source.ts_ms") / 1000)

    # Uma linha por pedido por lote: o ultimo evento (maior LSN) vence.
    janela = F.row_number().over(
        Window.partitionBy("pedido_id").orderBy(F.col("_lsn").desc())
    )

    colunas_tecnicas = [
        F.col("op").alias("_op"),
        F.col("source.lsn").alias("_lsn"),
        evento_em.alias("_evento_em"),
        agora.alias("_ingerido_em"),
    ]

    upserts = (
        eventos.where(F.col("op").isin("c", "u", "r"))
        .select("after.*", *colunas_tecnicas)
        .withColumn("_rn", janela)
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    deletes = (
        eventos.where(F.col("op") == "d")
        .select("before.*", *colunas_tecnicas)
        .withColumn("_rn", janela)
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    staged = upserts.unionByName(deletes, allowMissingColumns=True)
    staged.createOrReplaceTempView("staged")

    # A escrita acontece ANTES de qualquer marcacao de conclusao.
    spark.sql(montar_merge(DELTA_PATH))


def criar_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("cdc-pedidos-delta")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICO)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    (
        stream.writeStream.foreachBatch(aplicar_lote)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
