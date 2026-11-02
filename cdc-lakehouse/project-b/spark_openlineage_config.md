# OpenLineage Integration for Spark Job (Projeto B)

This script adds OpenLineage event emission to the existing Spark Structured Streaming job. 
It should be placed in `spark/job.py` (or imported from a separate module).

## Etapas de integração

### 1. Instalar a dependência
```bash
pip install openlineage-spark
```

### 2. Modificar `spark/job.py` (trechos a adicionar)

```python
from openlineage.spark import OpenLineageSparkDataset
from openlineage.client.emitters import LocalEmitter

# Inicializar emitter no SparkSession
spark = SparkSession.builder \
    .appName("cdc-lakehouse-spark") \
    .config("spark.openlineage.extract.append fed", "true") \
    .getOrCreate()

# Opcional: usar emitter local para desenvolvimento
emitter = LocalEmitter(namespace="cdc-lakehouse")

# Registrar UDFs ou wrappers que emitam events
# Exemplo: ao iniciar o streaming, emitir dataset de entrada
input_dataset = OpenLineageSparkDataset(
    name="loja.pedidos",
    fields=["pedido_id", "cliente_id", "preço", "status", "ingested_at"],
    descriptions={"pedido_id": "Chave primária da tabela de pedidos"},
)
emitter.emit("START", {"node": "spark_job", "runId": spark.sparkContext.applicationId, "timestamp": time.time()})

# No foreachBatch, após ler do Kafka e antes do MERGE, emitir dataset de saída
def foreach_batch_function(batch_df, batch_id):
    # ... processamento existente ...
    
    # Emitir dataset de saída do batch
    output_dataset = OpenLineageSparkDataset(
        name="delta.loja_pedidos",
        fields=["pedido_id", "cliente_id", "preço", "status", "ingested_at", "updated_at"],
        descriptions={"pedido_id": "Chave primária da tabela Delta"},
    )
    emitter.emit("COMPLETE", {"node": "spark_job", "runId": spark.sparkContext.applicationId, "timestamp": time.time()})

# Registrar o foreachBatch com o wrapper que emite events
# (depende da versão do openlineage-spark, geralmente se configura via spark.conf)
spark.conf.set("openlineage.schema.mode", "glossary")
```

### 3. Verificar eventos
- Ao submeter o job, os eventos aparecerão no dashboard do OpenLineage (ou Marquez, caso esteja integrado).
- No console Spark, deverá aparecer log similar a:
  ```
  2025-11-02 03:00:00 INFO [openlineage] Emitting START event for node spark_job
  2025-11-02 03:00:01 INFO [openlineage] Emitting COMPLETE event for node spark_job
  ```

### 4. (Opcional) Integrar com Marquez
- Se já usa Marquez, basta configurar o backend comum (Postgres/MySQL) e os events fluirão automaticamente.
- Caso contrário, o emitter local basta para gerar o lineage dentro do seu próprio ambiente.

## Arquivo sugerido: `spark/openlineage_config.py`

```python
from openlineage.client import set_local_emitter
from openlineage.client.emitters import LocalEmitter

# Configurar emitter local (desenvolvimento)
set_local_emitter(LocalEmitter(namespace="cdc-lakehouse"))
```

Basta importar este módulo no topo do `spark/job.py` antes de iniciar o SparkSession.
```