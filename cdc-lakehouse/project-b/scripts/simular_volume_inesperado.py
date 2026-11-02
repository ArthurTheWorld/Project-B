# Simular volume inesperado no pipeline CDC
# Este script injeta um lote extra de dados no Kafka topic para simular pico de volume

import os
import json
import base64
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICO = os.getenv("TOPICO", "loja.loja.pedidos")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))  # número de registros a injetar


def injetar_dados_extra():
    """Cria e injeta dados extras no topic Kafka."""
    print(f"📈 Simulando volume inesperado - injetando {BATCH_SIZE} registros extras...")

    # Gerar registros de exemplo
    import random
    extra_records = []
    for i in range(BATCH_SIZE):
        record = {
            "pedido_id": f"sim_{int(time.time())}_{i}",
            "cliente_id": f"cli_{random.randint(1, 1000)}",
            "preço": round(random.uniform(10, 500), 2),
            "quantidade": random.randint(1, 10),
            "status": random.choice(["pending", "processing", "completed"]),
            "source_ts_ms": int(time.time() * 1000),
        }
        extra_records.append(record)

    # Publicar no Kafka
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for record in extra_records:
        producer.send(TOPICO, value=record)
        print(f"   ✓ Enviado: {record['pedido_id']}")

    producer.flush()
    print(f"▶️  {BATCH_SIZE} registros extras injetados no topic {TOPICO}")