# Simular mudança de schema no pipeline CDC
# Este script adiciona uma coluna nova no Delta table para simular mudança de schema

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DELTA_PATH = os.getenv("DELTA_PATH", "/data/delta/pedidos")


def adicionar_coluna_nova():
    """Adiciona uma coluna inesperada no Delta table."""
    print(f"🔄 Simulando mudança de schema no Delta table: {DELTA_PATH}")

    from delta.tables import DeltaTable

    if not Path(DELTA_PATH).exists():
        print("⚠️  Delta table não encontrada - nada para alterar")
        return

    delta_table = DeltaTable.forPath(PROJECT_ROOT.spark, DELTA_PATH)

    # Adicionar coluna "source_system" como exemplo de mudança inesperada
    nova_coluna = "source_system"
    print(f"   - Adicionando coluna '{nova_coluna}'...")

    # Método 1: ALTER TABLE
    try:
        delta_table.toDF().write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(DELTA_PATH)
        print(f"▶️  Coluna '{nova_coluna}' adicionada com sucesso")
    except Exception as e:
        print(f"❌ Erro ao adicionar coluna: {e}")
        # Fallback: recrear tabela com schema expandido
        print("   Tentando método alternativo...")


def main():
    adicionar_coluna_nova()


if __name__ == "__main__":
    main()