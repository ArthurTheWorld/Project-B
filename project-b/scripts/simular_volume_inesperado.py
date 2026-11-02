"""Simula volume inesperado no pipeline CDC (Projeto B).

O script gera um lote extra de eventos CDC no formato que o Debezium publica
no tópico Kafka (envelope ``after``/``op``), para exercitar o SLO de volume.

Uso:
    python project-b/scripts/simular_volume_inesperado.py
    BATCH_SIZE=200 python project-b/scripts/simular_volume_inesperado.py

Envelope por mensagem (mesmo contrato de ``spark/job.py``):
    {"op": "c"|"u"|"d", "ts_ms": <int>, "after": {...} | null, "before": {...} | null}

O campo ``after`` segue o schema real de ``loja.pedidos``:
    pedido_id, cliente_id, status, valor, canal, criado_em, atualizado_em
"""

from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICO = os.getenv("TOPICO", "loja.loja.pedidos")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

STATUS_VALIDOS = ("novo", "pago", "enviado", "entregue", "cancelado")
CANAIS_VALIDOS = ("app", "web", "loja", "telefone")


def _timestamp_iso(ms: int) -> str:
    """Converte epoch-ms para o formato ISO-8601 usado pelo Postgres."""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + ".000Z"


def gerar_evento(indice: int) -> dict:
    """Monta um envelope CDC compatível com a tabela loja.pedidos."""
    agora_ms = int(time.time() * 1000)
    pedido_id = 900000 + indice
    criado_ms = agora_ms - random.randint(0, 86_400_000)
    linha = {
        "pedido_id": pedido_id,
        "cliente_id": random.randint(1, 1000),
        "status": random.choice(STATUS_VALIDOS),
        "valor": round(random.uniform(10.0, 500.0), 2),
        "canal": random.choice(CANAIS_VALIDOS),
        "criado_em": _timestamp_iso(criado_ms),
        "atualizado_em": _timestamp_iso(agora_ms),
    }
    return {
        "op": "c",
        "ts_ms": agora_ms,
        "before": None,
        "after": linha,
    }


def injetar_dados_extra() -> int:
    """Publica os eventos extras no tópico Kafka e devolve quantos foram enviados."""
    from kafka import KafkaProducer  # import tardio: só é exigido quando se roda de verdade

    print(f"Simulando volume inesperado — injetando {BATCH_SIZE} eventos CDC em {TOPICO}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    enviados = 0
    for i in range(BATCH_SIZE):
        evento = gerar_evento(i)
        producer.send(TOPICO, value=evento)
        enviados += 1
    producer.flush()
    print(f"{enviados} eventos injetados no tópico {TOPICO}")
    return enviados


def main() -> int:
    try:
        injetar_dados_extra()
    except ModuleNotFoundError:
        print(
            "kafka-python não está instalado. Rode dentro do container do gerador:\n"
            "  docker compose exec generator python /scripts/simular_volume_inesperado.py",
        )
        return 2
    except Exception as erro:  # noqa: BLE001 - falha de broker deve ser reportada, não escondida
        print(f"Falha ao publicar no Kafka: {erro}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())