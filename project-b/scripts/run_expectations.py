"""Executa as suítes de expectations do Projeto B.

Uso:
    python project-b/scripts/run_expectations.py            # roda todas as suítes
    python project-b/scripts/run_expectations.py --amostra  # usa dados sintéticos válidos

O script importa os módulos de ``project-b/expectations`` e roda as
expectations em memória. Ele NÃO depende de great_expectations nem de Spark —
apenas stdlib — para rodar no CI sem infraestrutura.

Códigos de saída:
    0  todas as expectations passaram
    1  pelo menos uma expectation falhou
    2  erro de importação/configuração da suíte
"""

from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTATIONS_DIR = PROJECT_ROOT / "expectations"

if str(EXPECTATIONS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPECTATIONS_DIR))

import mercado_expectations  # noqa: E402
import pedidos_expectations  # noqa: E402


@dataclass
class SuiteConfig:
    """Liga uma suíte à sua fonte de dados."""

    nome: str
    fabrica: Callable[[], Any]
    amostra: list[dict[str, Any]]


def _amostra_pedidos() -> list[dict[str, Any]]:
    """Linhas sintéticas que respeitam o schema real de loja.pedidos."""
    base = {
        "cliente_id": 7,
        "canal": "app",
        "criado_em": "2026-11-02T10:00:00Z",
        "atualizado_em": "2026-11-02T10:05:00Z",
        "_op": "u",
        "_lsn": 100,
        "_evento_em": "2026-11-02T10:05:00Z",
        "_ingerido_em": "2026-11-02T10:05:03Z",
    }
    return [
        {"pedido_id": 1, "status": "novo", "valor": 49.9, **base},
        {"pedido_id": 2, "status": "pago", "valor": 120.0, **base},
        {"pedido_id": 3, "status": "entregue", "valor": 15.5, **base},
    ]


def _amostra_mercado() -> list[dict[str, Any]]:
    """Linhas sintéticas que respeitam o staging de mercado."""
    return [
        {"moeda": "BTC", "preco": 348.50, "volume": 1200.5, "timestamp": "2026-11-02T08:00:00Z"},
        {"moeda": "ETH", "preco": 182.04, "volume": 8400.0, "timestamp": "2026-11-02T08:00:00Z"},
    ]


def _configuracoes() -> list[SuiteConfig]:
    return [
        SuiteConfig(
            nome="pedidos_expectations",
            fabrica=pedidos_expectations.create_pedidos_suite,
            amostra=_amostra_pedidos(),
        ),
        SuiteConfig(
            nome="mercado_expectations",
            fabrica=mercado_expectations.create_mercado_suite,
            amostra=_amostra_mercado(),
        ),
    ]


def executar_suite(config: SuiteConfig, linhas: Iterable[Mapping[str, Any]]) -> bool:
    """Roda uma suíte e imprime o resultado de cada expectation."""
    suite = config.fabrica()
    resultados = suite.validar(list(linhas))

    print(f"\n=== {suite.nome}  (tabela: {suite.tabela})  — {len(resultados)} expectations ===")
    falhas = 0
    for resultado in resultados:
        marca = "PASS" if resultado.sucesso else "FAIL"
        alvo = f" [{resultado.coluna}]" if resultado.coluna else ""
        detalhe = f" — {resultado.detalhe}" if resultado.detalhe else ""
        print(f"  [{marca}] {resultado.nome}{alvo}{detalhe}")
        if not resultado.sucesso:
            falhas += 1

    print(f"  -> {len(resultados) - falhas}/{len(resultados)} passaram")
    return falhas == 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Executa as suítes de expectations do Projeto B.")
    parser.add_argument(
        "--amostra",
        action="store_true",
        help="valida as suítes contra amostras sintéticas conhecidas (padrão quando não há fonte real)",
    )
    # Mantido por compatibilidade: o CI chama sem argumentos.
    parser.parse_args(argv)

    try:
        configuracoes = _configuracoes()
    except Exception:  # noqa: BLE001 - falha de importação/configuração vale exit 2
        print("Erro ao carregar as suítes de expectations:", file=sys.stderr)
        traceback.print_exc()
        return 2

    todos_ok = True
    for config in configuracoes:
        if not executar_suite(config, config.amostra):
            todos_ok = False

    print("\nResultado geral:", "OK" if todos_ok else "FALHOU")
    return 0 if todos_ok else 1


if __name__ == "__main__":
    sys.exit(main())