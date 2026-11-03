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
    """Recebe o batch (DataFrame do Kafka, com a coluna `value`) e aplica no Delta.

    O Debezium publica o evento como JSON no `value` do registro Kafka.
    A coluna `value` chega como binario; decodificamos para string antes de
    parsear com spark.read.json.
    """
    spark = lote.sparkSession

    brutos = [
        linha["value"].decode("utf-8") if isinstance(linha["value"], (bytes, bytearray)) else linha["value"]
        for linha in lote.select("value").collect()
        if linha["value"]
    ]
    if not brutos:
        return

    eventos = spark.read.json(spark.sparkContext.parallelize(brutos))

    # Instrumentacao: timestamps de evento e de ingestao
    agora = F.current_timestamp()
    evento_em = F.to_timestamp(F.col("source.ts_ms") / 1000)

    colunas_tecnicas = [
        F.col("op").alias("_op"),
        F.col("source.lsn").alias("_lsn"),
        evento_em.alias("_evento_em"),
        agora.alias("_ingerido_em"),
    ]

    # Normaliza c/u/r (usa `after`) e d (usa `before`) num unico shape, mantendo
    # o LSN para ordenar. O MERGE exige NO MAXIMO UMA linha de origem por chave:
    # o mesmo pedido pode ter c, u e d dentro do mesmo microbatch de 10s.
    upserts = eventos.where(F.col("op").isin("c", "u", "r")).select(
        "after.*", *colunas_tecnicas
    )
    deletes = eventos.where(F.col("op") == "d").select(
        "before.*", *colunas_tecnicas
    )

    # Instrumentacao de diagnostico: separa "delete nao chega do Kafka" de
    # "delete chega e e descartado aqui". Remover quando o caminho estiver provado.
    n_eventos = eventos.count()
    n_upserts = upserts.count()
    n_deletes = deletes.count()
    n_deletes_com_pk = deletes.where(F.col("pedido_id").isNotNull()).count()
    print(
        f"[batch {batch_id}] eventos={n_eventos} upserts={n_upserts} "
        f"deletes={n_deletes} deletes_com_pk={n_deletes_com_pk}",
        flush=True,
    )

    todos = upserts.unionByName(deletes, allowMissingColumns=True)

    # Uma linha por pedido por lote: o ultimo evento (maior LSN) vence.
    # Ordenar por _lsn desc garante que o estado final do pedido seja o aplicado.
    janela = Window.partitionBy("pedido_id").orderBy(F.col("_lsn").desc())
    staged = (
        todos.withColumn("_rn", F.row_number().over(janela))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

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
