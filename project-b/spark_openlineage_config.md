# OpenLineage no job Spark (Projeto B)

Este documento descreve como o lineage é obtido no pipeline CDC. Ele foi
reescrito porque a versão anterior descrevia uma integração que **não existe**
e que derrubava o job.

## O erro da versão anterior

O texto antigo mandava colocar isto no topo de `spark/job.py`:

```python
from openlineage.spark import OpenLineageSparkDataset
from openlineage.client.emitters import LocalEmitter
```

Isso quebra o job por dois motivos independentes:

1. **O pacote é Scala, não Python.** `openlineage-spark` é um JAR publicado no
   Maven Central (`io.openlineage:openlineage-spark_2.12`). Não existe módulo
   Python chamado `openlineage.spark` no classpath do Spark.
2. **`LocalEmitter` não existe mais.** Foi removido na linha 1.x do cliente
   Python, junto com `set_local_emitter`. O import levanta `ImportError`
   imediatamente, na subida do container — antes de qualquer batch rodar.

Resultado prático: o job morria no boot com `ImportError`, e o pipeline CDC
ficava parado com o OpenLineage *desligado*. A observabilidade derrubou o que
deveria observar.

## Como funciona de verdade

Em Spark, OpenLineage é um **listener** registrado via `spark.extraListeners`.
O JAR é baixado pelo próprio Spark com `--packages` e não requer nenhuma
mudança no código Python do job.

Já implementado em `docker-compose.yml` (serviço `spark`):

```bash
PACOTES="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
LINEAGE=""
if [ "${OPENLINEAGE_ENABLED:-false}" = "true" ]; then
  LINEAGE="--packages io.openlineage:openlineage-spark_2.12:1.9.1 \
    --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
    --conf spark.openlineage.namespace=cdc-lakehouse \
    --conf spark.openlineage.transport.type=console"
fi
```

### Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `OPENLINEAGE_ENABLED` | `false` | Liga/desliga o listener. O job roda igual com ele desligado. |
| `OPENLINEAGE_NAMESPACE` | `cdc-lakehouse` | Namespace dos jobs no backend de lineage. |
| `OPENLINEAGE_TRANSPORT` | `console` | `console`, `http`, `file`… |
| `OPENLINEAGE_URL` | vazio | Endpoint do backend (Marquez etc.) quando o transporte é `http`. |
| `OPENLINEAGE_PACKAGE` | `io.openlineage:openlineage-spark_2.12:1.9.1` | Coordenada Maven do JAR. |

### Ligando localmente

```powershell
$env:OPENLINEAGE_ENABLED = "true"
docker compose up -d spark
docker compose logs -f spark   # os eventos aparecem como JSON no stdout
```

Para enviar a um Marquez em vez do console:

```powershell
$env:OPENLINEAGE_ENABLED = "true"
$env:OPENLINEAGE_TRANSPORT = "http"
$env:OPENLINEAGE_URL = "http://marquez:5000"
docker compose up -d spark
```

## Garantia que o projeto assume

O requisito duro é: **o job nunca pode quebrar por causa do OpenLineage.**
Isso é verificado estaticamente por `tests/test_job_contrato.py`:

- `test_job_nao_importa_openlineage_em_python` — proíbe qualquer `import openlineage` em `spark/`.
- `test_compose_e_opt_in_para_openlineage` — o compose só adiciona o listener quando `OPENLINEAGE_ENABLED=true`.

O que **não** está provado: que os eventos chegam corretamente ao backend com
o Docker rodando. Essa verificação exige subir a stack e inspecionar o stdout
do container `spark`.

## Dataset esperado

Com o listener ligado, cada micro-lote emite eventos para:

- **entrada**: tópico Kafka `loja.loja.pedidos`
- **saída**: tabela Delta em `DELTA_PATH` (`/data/delta/pedidos`)

Os campos do dataset de saída são os do schema real — `pedido_id`, `cliente_id`,
`status`, `valor`, `canal`, `criado_em`, `atualizado_em` — mais as colunas
técnicas `_op`, `_lsn`, `_evento_em`, `_ingerido_em`. Não existem `preço`,
`quantidade` nem `ingested_at` nesta tabela (esses são do projeto de cripto).