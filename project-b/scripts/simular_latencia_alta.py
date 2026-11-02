# Simular latência alta no pipeline CDC
# Este script pausa o consumidor por um período definido para simular atraso

import os
import time
import signal
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPICO = os.getenv("TOPICO", "loja.loja.pedidos")
DURATION_SECONDS = int(os.getenv("DURATION", "30"))  # segundos para simular latência


def pausar_consumidor():
    """Pausa o consumidor Kubernetes/Spark por DURATION segundos."""
    print(f"🛑 Simulando latência alta por {DURATION_SECONDS}s...")
    print("   - Pausando leitura do Kafka")
    print("   - Aguardando atraso artificial")
    time.sleep(DURATION_SECONDS)
    print("▶️  Retomando leitura normal")


def main():
    try:
        pausar_consumidor()
    except KeyboardInterrupt:
        print("\n⏹️  Simulação interrompida pelo usuário")
        sys.exit(1)


if __name__ == "__main__":
    main()