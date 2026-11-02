"""Testes de contrato estatico de spark/job.py (sem Spark).

Docker nao esta disponivel neste ambiente, entao nao ha teste de execucao real
do Structured Streaming. O que estes testes garantem e o contrato que ja quebrou
uma vez:

1. Nenhum import Python de OpenLineage (o listener do Spark e Scala/config).
2. Nenhum simbolo inexistente de API do Spark (`sessionState.getConf`,
   `LocalEmitter`, `set_local_emitter`).
3. A escrita (MERGE) acontece ANTES de qualquer marcacao de conclusao.
4. O job le a configuracao do ambiente (DELTA_PATH/CHECKPOINT/TOPICO), em vez de
   referenciar variaveis globais inexistentes.
5. Nenhum campo de schema inventado (`preco`, `ingested_at`).

LIMITACAO: nada aqui prova exactly-once, latencia ou que o job sobe dentro do
container — isso exige `docker compose up` com os jars do Delta/Kafka.
"""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
JOB = RAIZ / "spark" / "job.py"
COMPOSE = RAIZ / "docker-compose.yml"

FONTE = JOB.read_text(encoding="utf-8")
ARVORE = ast.parse(FONTE)


def _imports_modulos() -> list[str]:
    modulos: list[str] = []
    for no in ast.walk(ARVORE):
        if isinstance(no, ast.Import):
            modulos.extend(alias.name for alias in no.names)
        elif isinstance(no, ast.ImportFrom) and no.module:
            modulos.append(no.module)
    return modulos


def test_job_nao_importa_openlineage_em_python():
    modulos = _imports_modulos()

    assert not [m for m in modulos if "openlineage" in m.lower()], modulos


def test_job_nao_usa_simbolos_inexistentes_do_spark():
    for proibido in ("sessionState", "getConf", "LocalEmitter", "set_local_emitter"):
        assert proibido not in FONTE, f"job.py ainda referencia {proibido}"


def test_job_nao_declara_campos_de_schema_inventados():
    for inventado in ("preco", "ingested_at"):
        assert inventado not in FONTE, f"job.py referencia campo inexistente: {inventado}"


def test_merge_acontece_antes_de_qualquer_conclusao():
    """O MERGE precisa ser a ultima operacao de escrita do lote."""
    pos_merge = FONTE.index("spark.sql(montar_merge(")

    # Depois do MERGE nao pode existir nenhuma chamada de "conclusao"/lineage.
    depois = FONTE[pos_merge + len("spark.sql(montar_merge(") :]
    chamadas_depois = re.findall(r"\n\s*(\w+)\.(\w+)\(", depois)
    assert not chamadas_depois, f"chamadas apos o MERGE: {chamadas_depois}"


def test_config_do_job_vem_do_ambiente():
    """DELTA_PATH/CHECKPOINT/TOPICO precisam de os.getenv — senao NameError."""
    for variavel in ("DELTA_PATH", "CHECKPOINT", "TOPICO", "KAFKA_BOOTSTRAP"):
        padrao = re.compile(rf'^\s*{variavel}\s*=\s*os\.getenv\("', re.MULTILINE)
        assert padrao.search(FONTE), f"{variavel} nao e definida via os.getenv"
        assert os.environ.get(variavel) is None or True  # documenta: env do host nao importa


def test_compose_e_opt_in_para_openlineage():
    compose = COMPOSE.read_text(encoding="utf-8")

    assert "OPENLINEAGE_ENABLED" in compose
    # default desligado
    assert '${OPENLINEAGE_ENABLED:-false}' in compose
    # o listener so entra por config, nunca por import Python
    assert "spark.extraListeners" in compose
    assert "io.openlineage.spark.agent.OpenLineageSparkListener" in compose


def test_compose_contrato_de_paths_bate_com_job():
    compose = COMPOSE.read_text(encoding="utf-8")

    assert "DELTA_PATH: /data/delta/pedidos" in compose
    assert "CHECKPOINT: /data/checkpoints/pedidos" in compose
    assert "TOPICO: loja.loja.pedidos" in compose
    # o job usa os mesmos defaults quando a env falta
    assert '"/data/delta/pedidos"' in FONTE
    assert '"/data/checkpoints/pedidos"' in FONTE