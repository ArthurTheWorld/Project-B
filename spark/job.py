"""Consumo CDC -> Delta Lake com MERGE idempotente.

O problema que este job resolve
-------------------------------
O Kafka entrega o evento, mas o lake precisa refletir o *estado atual* da
tabela: um pedido que mudou 10 vezes é uma linha no lake, não 10. E se o
job reiniciar e reprocessar o mesmo lote, o resultado tem que ser o mesmo.

Solução: cada micro-lote é aplicado com um `MERGE` por chave primária.
- op 'c'/'u'/'r' -> upsert
- op 'd'         -> delete
Como o merge é por chave, reprocessar é inofensivo: o estado final converge.

Evolução de schema
------------------
`spark.databricks.delta.schema.autoMerge.enabled=true` faz o merge aceitar
colunas novas que apareceram na origem. É o teste que prova que adicionar
uma coluna no Postgres não derruba o pipeline.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from merge_sql import montar_merge

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICO = os.getenv("TOPICO", "loja.loja.pedidos")
DELTA_PATH = os.getenv("DELTA_PATH", "/data/delta/pedidos")
CHECKPOINT = os.getenv("CHECKPOINT", "/data/checkpoints/pedidos")


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


def aplicar_lote(lote, batch_id: int) -> None:
    """Recebe o batch (DataFrame com a coluna `raw`) e aplica no Delta.

    Assinatura obrigatória do `foreachBatch`: (DataFrame, batch_id).
    """
    spark = lote.sparkSession

    brutos = [linha["raw"] for linha in lote.select("raw").collect() if linha["raw"]]
    if not brutos:
        return

    eventos = spark.read.json(spark.sparkContext.parallelize(brutos))

    # Uma linha por pedido por lote: o último evento (maior LSN) vence.
    # A ordenação usa `_lsn` porque, depois do select, `source` deixou de existir.
    janela = F.row_number().over(
        Window.partitionBy("pedido_id").orderBy(F.col("_lsn").desc())
    )

    upserts = (
        eventos.where(F.col("op").isin("c", "u", "r"))
        .select("after.*", F.col("op").alias("_op"), F.col("source.lsn").alias("_lsn"))
        .withColumn("_rn", janela)
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    deletes = (
        eventos.where(F.col("op") == "d")
        .select("before.*", F.col("op").alias("_op"), F.col("source.lsn").alias("_lsn"))
        .withColumn("_rn", janela)
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    staged = upserts.unionByName(deletes, allowMissingColumns=True)
    staged.createOrReplaceTempView("staged")

    if not os.path.exists(f"{DELTA_PATH}/_delta_log"):
        # Primeira carga: cria a tabela já com o schema do primeiro lote.
        print(f"[batch] criando tabela Delta em {DELTA_PATH}")
        staged.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            DELTA_PATH
        )
        return

    print(f"[batch] aplicando MERGE ({staged.count()} linhas)")

    spark.sql(montar_merge(DELTA_PATH))


def main() -> None:
    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"lendo {TOPICO} de {BOOTSTRAP} -> {DELTA_PATH}")

    fluxo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPICO)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("raw"))
        .writeStream.foreachBatch(aplicar_lote)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    fluxo.awaitTermination()


if __name__ == "__main__":
    main()
