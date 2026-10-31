"""Testes do SQL de merge (sem Spark)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))

from merge_sql import montar_merge  # noqa: E402


def test_merge_usa_chave_primaria():
    sql = montar_merge("/data/delta/pedidos")

    assert "MERGE INTO delta.`/data/delta/pedidos`" in sql
    assert "alvo.pedido_id = origem.pedido_id" in sql


def test_merge_trata_delete_antes_do_update():
    sql = montar_merge("/tmp/x")

    assert "THEN DELETE" in sql
    assert sql.index("THEN DELETE") < sql.index("UPDATE SET *")


def test_merge_nao_insere_registro_de_delete():
    sql = montar_merge("/tmp/x")

    assert "WHEN NOT MATCHED AND origem._op <> 'd' THEN INSERT *" in sql
