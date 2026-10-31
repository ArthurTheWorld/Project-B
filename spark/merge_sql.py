"""Montagem do SQL de MERGE — isolado para ser testável sem Spark."""

from __future__ import annotations


def montar_merge(caminho_delta: str) -> str:
    """SQL que aplica o estado atual da origem no Delta.

    - op 'd'  -> remove a linha (CDC de DELETE)
    - demais  -> upsert por chave primária

    Como a chave é a mesma da origem, reprocessar um lote não duplica nada:
    o estado final é idempotente por construção.
    """
    return f"""
        MERGE INTO delta.`{caminho_delta}` AS alvo
        USING staged AS origem
        ON alvo.pedido_id = origem.pedido_id
        WHEN MATCHED AND origem._op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND origem._op <> 'd' THEN INSERT *
        """
