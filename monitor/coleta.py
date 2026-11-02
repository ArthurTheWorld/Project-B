"""Coleta de métricas do pipeline CDC.

Roda as consultas do Delta em um container Spark efêmero e devolve JSON.
Foi feito assim (e não com `deltalake`/DuckDB no processo Python) por um motivo
simples: o container do Spark já tem os jars do Delta resolvidos. Um leitor
Python exigiria mais uma dependência binária e uma segunda forma de ler a mesma
tabela — duas fontes de verdade para o mesmo número.

O custo é a latência de subir um Spark a cada coleta (~4 s), por isso o painel
consulta em intervalos, e não a cada segundo.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

RAIZ = Path(__file__).resolve().parents[1]

SPARK_SQL = [
    "/opt/spark/bin/spark-sql",
    "--packages",
    "io.delta:delta-spark_2.12:3.2.0",
    "--conf",
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf",
    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]

# Spark polui a saída com WARN; só as linhas tabulares interessam.
_RUIDO = re.compile(r"^\d{2}/\d{2}/\d{2}|^\s*$|artifacts copied|WARN|INFO")


@dataclass
class Metricas:
    """Fotografia do pipeline em um instante."""

    coletado_em: str
    linhas: int = 0
    latencia_media_s: float | None = None
    latencia_p50_s: float | None = None
    latencia_p95_s: float | None = None
    latencia_max_s: float | None = None
    vazao_por_minuto: float = 0.0
    por_operacao: dict[str, int] | None = None
    por_status: dict[str, int] | None = None
    serie_minuto: list[dict[str, Any]] | None = None
    origem_linhas: int | None = None
    erro: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _rodar(consulta: str, timeout: int = 180) -> str:
    """Executa SQL no container do Spark e devolve a saída crua."""
    comando = ["docker", "compose", "exec", "-T", "spark", *SPARK_SQL, "-e", consulta]
    resultado = subprocess.run(
        comando,
        cwd=RAIZ,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if resultado.returncode != 0 and not resultado.stdout:
        raise RuntimeError(resultado.stderr.strip()[:300])
    return resultado.stdout


def _linhas_tabela(saida: str) -> list[list[str]]:
    """Extrai as linhas de dados (ignora cabeçalho, warnings e separador)."""
    linhas: list[list[str]] = []
    for bruta in saida.splitlines():
        if _RUIDO.match(bruta) or bruta.startswith("+") or set(bruta) <= {"-", "+", "="}:
            continue
        partes = [p.strip() for p in bruta.split("\t") if p.strip()]
        if len(partes) >= 2 or (len(partes) == 1 and re.fullmatch(r"-?[\d.]+", partes[0])):
            linhas.append(partes)
    return linhas


def _float(valor: str) -> float | None:
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


VIEW = "create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');"


def coletar(origem_linhas: int | None = None) -> Metricas:
    """Coleta o estado atual do pipeline."""
    metricas = Metricas(coletado_em=datetime.now(timezone.utc).isoformat())

    try:
        # Contagem total do lake (TODAS as linhas), separada da amostra de latência.
        saida_total = _rodar(f"{VIEW} select count(*) as linhas from pedidos;")
        for linha in _linhas_tabela(saida_total):
            if linha and re.fullmatch(r"\d+", linha[0]):
                metricas.linhas = int(linha[0])
                break

        # Latência só faz sentido nas linhas que já têm os dois carimbos de tempo.
        saida = _rodar(
            f"""
            {VIEW}
            select
                round(avg(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2),
                round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.5), 2),
                round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.95), 2),
                round(max(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2)
            from pedidos
            where _evento_em is not null and _ingerido_em is not null;
            """
        )
        tabela = _linhas_tabela(saida)
        if tabela:
            valores = tabela[-1]
            # A saída pode vir como uma única linha com tabulações
            if len(valores) == 1:
                valores = valores[0].split()
            if len(valores) >= 4:
                metricas.latencia_media_s = _float(valores[0])
                metricas.latencia_p50_s = _float(valores[1])
                metricas.latencia_p95_s = _float(valores[2])
                metricas.latencia_max_s = _float(valores[3])

        saida_op = _rodar(
            f"{VIEW} select _op, count(*) as total from pedidos group by _op order by total desc;"
        )
        metricas.por_operacao = {
            linha[0]: int(_float(linha[1]) or 0)
            for linha in _linhas_tabela(saida_op)
            if len(linha) >= 2
        }

        saida_status = _rodar(
            f"{VIEW} select status, count(*) as total from pedidos group by status order by total desc;"
        )
        metricas.por_status = {
            linha[0]: int(_float(linha[1]) or 0)
            for linha in _linhas_tabela(saida_status)
            if len(linha) >= 2
        }

        saida_serie = _rodar(
            f"""
            {VIEW}
            select date_trunc('minute', _ingerido_em) as minuto, count(*) as linhas
            from pedidos
            where _ingerido_em >= current_timestamp() - interval '15 minutes'
            group by 1 order by 1;
            """
        )
        serie = []
        for linha in _linhas_tabela(saida_serie):
            if len(linha) >= 2:
                serie.append({"minuto": linha[0], "linhas": int(_float(linha[1]) or 0)})
        metricas.serie_minuto = serie
        if len(serie) >= 2:
            metricas.vazao_por_minuto = sum(p["linhas"] for p in serie[:-1]) / max(1, len(serie) - 1)

    except Exception as exc:  # noqa: BLE001 - o painel precisa sobreviver a falha
        log.exception("falha na coleta")
        metricas.erro = str(exc)[:300]

    metricas.origem_linhas = origem_linhas
    return metricas


def coletar_origem() -> int | None:
    """Contagem no Postgres, para comparar com o lake."""
    try:
        resultado = subprocess.run(
            [
                "docker", "compose", "exec", "-T", "postgres",
                "psql", "-U", "loja", "-d", "loja", "-t", "-A",
                "-c", "select count(*) from loja.pedidos;",
            ],
            cwd=RAIZ,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return int(resultado.stdout.strip())
    except Exception:  # noqa: BLE001
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(coletar(coletar_origem()).to_dict(), indent=2, ensure_ascii=False))
