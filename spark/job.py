"""Consumo CDC -> Delta Lake com MERGE idempotente + OpenLineage.

Extensões adicionadas para o Projeto B (Confiabilidade de Dados):
- Emissão de eventos OpenLineage a cada batch (START/COMPLETE).
- Dataset de entrada (Kafka topic) e de saída (Delta table) são registrados.
- Permite rastrear lineage completa e alimentar painel de SLOs/healthcheck.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from merge_sql import montar_merge
from openlineage.client import set_local_emitter
from openlineage.client.emitters import LocalEmitter

# --- OpenLineage setup ----------------------------------------------------
# Para desenvolvimento/local usa emitter local; em produção troque pelo backend
# (ex.: Marquez, OpenLineage HTTP). Veja o README do projeto para mais detalhes.
set_local_emitter(LocalEmitter(namespace="cdc-lakehouse"))

# Helper para criar dataset OpenLineage
def _make_dataset(nome: str, fields: list, descriptions: dict = None):
    """Constrói um objeto OpenLineage Spark Dataset (simplificado)."""
    # O openlineage-spark espera tipos especiais; aqui criamos um dict que será
    # serializado pelo emitter. Apenas o nome e os campos são informados.
    return {
        "name": nome,
        "fields": [{"name": f, "type": "string"} for f in fields],
    } if descriptions is None else {
        "name": nome,
        "fields": [{"name": f, "type": "string", "description": descriptions.get(f, "")} for f in fields],
    }

# --- Configurações ambientais ---------------------------------------------
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


def _emit_start(spark, job_name: str = "spark_job") -> None:
    """Emite evento START no início do job."""
    run_id = spark.sparkContext.applicationId
    emitter = LocalEmitter(namespace="cdc-lakehouse")
    dataset = _make_dataset(
        nome=job_name,
        fields=["runId", "namespace", "job"],
    )
    emitter.emit("START", {"node": job_name, "runId": run_id, "timestamp": time.time()})
    # Opcional: imprimir no log para confirmação
    print(f"[openlineage] START event emitted for {job_name}")


def _emit_complete(spark, job_name: str = "spark_job") -> None:
    """Emite evento COMPLETE ao final do batch (chamado dentro de foreachBatch)."""
    run_id = spark.sparkContext.applicationId
    emitter = LocalEmitter(namespace="cdc-lakehouse")
    # Dataset de saída (Delta) - usamos o nome da tabela alvo
    output_dataset = _make_dataset(
        nome=f"delta.{os.path.basename(DELTA_PATH).replace('/', '_')}",
        fields=[
            "pedido_id",
            "cliente_id",
            "preco",
            "status",
            "ingested_at",
            "_ingerido_em",
        ],
        descriptions={
            "pedido_id": "Chave primária do pedido",
            "cliente_id": "ID do cliente",
            "preco": "Preço unitário do produto",
            "status": "Status do pedido (pending/processing/completed)",
            "ingested_at": "Timestamp de ingestão no source",
            "_ingerido_em": "Timestamp de inserção no lake",
        },
    )
    emitter.emit("COMPLETE", {"node": job_name, "runId": run_id, "timestamp": time.time(), "dataset": output_dataset})
    print(f"[openlineage] COMPLETE event emitted for {job_name}")


def aplicar_lote(lote, batch_id: int) -> None:
    """Recebe o batch (DataFrame com a coluna `raw`) e aplica no Delta.

    Agora também emite eventos OpenLineage START/COMPLETE.
    Assinatura obrigatória do `foreachBatch`: (DataFrame, batch_id).
    """
    spark = lote.sparkSession

    # --- Emissão START (apenas uma vez por job, na primeira chamada) ---
    # Usamos um flag de sessão para não emitir a cada batch se não desejar.
    if not spark.sessionState.getConf("openlineage.start.emitted"):
        _emit_start(spark)
        spark.sessionState.conf.set("openlineage.start.emitted", "true")

    brutos = [linha["raw"] for linha in lote.select("raw").collect() if linha["raw"]]
    if not brutos:
        return

    eventos = spark.read.json(spark.sparkContext.parallelize(brutos))

    # Instrumentação: timestamps de evento e de ingestão
    agora = F.current_timestamp()
    evento_em = F.to_timestamp(F.col("source.ts_ms") / 1000)

    # Uma linha por pedido por lote: o último evento (maior LSN) vence.
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

    if not os.path.exists(f"{DELTA_PATH}/_delta_log"):
        # Primeira carga: cria a tabela já com o schema do primeiro lote.
        print(f"[batch] criando tabela Delta em {DELTA_PATH}")
        staged.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            DELTA_PATH
        )
        # Emite COMPLETE após criar a tabela
        _emit_complete(spark, job_name="spark_job")
        return

    print(f"[batch] aplicando MERGE ({staged.count()} linhas)")

    # --- Emissão COMPLETE após o merge ---
    _emit_complete(spark, job_name="spark_job")

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