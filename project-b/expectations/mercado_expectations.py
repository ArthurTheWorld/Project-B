"""Suíte de expectations para stg_coingecko__mercado (Projeto B).

Este arquivo é Python executável de verdade — não um documento Markdown.

A tabela ``stg_coingecko__mercado`` vem do projeto de cripto (crypto-ELT) e
alimenta o segundo SLO do catálogo. Diferente de ``loja.pedidos``, ela é
carregada a cada 2h e admite evolução de schema (``allow_add_column: true``).

Colunas esperadas (fonte: ``slo_catalog.yaml`` + dbt staging):

    moeda, preco, volume, timestamp

Os nomes estão em português porque o restante do repositório também está —
o staging do dbt renomeia ``price`` → ``preco`` e ``snapshot_at`` → ``timestamp``.

Por que não usar great_expectations aqui: o pacote não está instalado no
ambiente e o CI do projeto roda sem Spark. As expectations abaixo são
declarativas e verificáveis com stdlib puro.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping


COLUNAS_OBRIGATORIAS: tuple[str, ...] = ("moeda", "preco", "volume", "timestamp")

# Moedas acompanhadas pelo pipeline de mercado.
MOEDAS_VALIDAS: tuple[str, ...] = ("BTC", "ETH", "BNB", "ADA", "DOGE", "SOL")

# Intervalo de preço esperado, coerente com slo_catalog.yaml (0..1000 USD).
PRECO_MINIMO = 0.0
PRECO_MAXIMO = 1000.0

# Volume nunca pode ser negativo.
VOLUME_MINIMO = 0.0


@dataclass(frozen=True)
class ResultadoExpectation:
    """Resultado de uma expectation individual."""

    nome: str
    coluna: str | None
    sucesso: bool
    detalhe: str = ""


@dataclass
class SuiteMercado:
    """Suíte de expectativas para a tabela stg_coingecko__mercado."""

    nome: str = "mercado_expectations"
    tabela: str = "stg_coingecko__mercado"
    colunas_obrigatorias: tuple[str, ...] = COLUNAS_OBRIGATORIAS
    moedas_validas: tuple[str, ...] = MOEDAS_VALIDAS
    preco_minimo: float = PRECO_MINIMO
    preco_maximo: float = PRECO_MAXIMO
    volume_minimo: float = VOLUME_MINIMO
    expectations: list[Callable[[Iterable[Mapping[str, Any]]], ResultadoExpectation]] = field(
        default_factory=list
    )

    def __post_init__(self) -> None:
        if not self.expectations:
            self.expectations = [
                self.colunas_esperadas_presentes,
                self.moeda_no_dominio,
                self.preco_dentro_do_intervalo,
                self.volume_nao_negativo,
                self.timestamp_nao_nulo,
                self.uma_linha_por_moeda_e_timestamp,
            ]

    # ------------------------------------------------------------------ #
    # Expectations
    # ------------------------------------------------------------------ #
    def colunas_esperadas_presentes(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """Toda linha tem as colunas obrigatórias do staging."""
        esperadas = set(self.colunas_obrigatorias)
        faltando: set[str] = set()
        for linha in linhas:
            faltando |= esperadas - set(linha.keys())
            if faltando:
                break
        return ResultadoExpectation(
            nome="colunas_esperadas_presentes",
            coluna=None,
            sucesso=not faltando,
            detalhe="" if not faltando else f"colunas ausentes: {sorted(faltando)}",
        )

    def moeda_no_dominio(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """moeda pertence ao conjunto acompanhado pelo pipeline."""
        invalidos: dict[Any, int] = {}
        for linha in linhas:
            valor = linha.get("moeda")
            if valor not in self.moedas_validas:
                invalidos[valor] = invalidos.get(valor, 0) + 1
        return ResultadoExpectation(
            nome="moeda_no_dominio",
            coluna="moeda",
            sucesso=not invalidos,
            detalhe="" if not invalidos else f"fora do domínio: {invalidos}",
        )

    def preco_dentro_do_intervalo(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """preco é numérico e cai no intervalo do SLO (0..1000)."""
        fora = 0
        nao_numerico = 0
        for linha in linhas:
            valor = linha.get("preco")
            try:
                numero = float(valor)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                nao_numerico += 1
                continue
            if not (self.preco_minimo <= numero <= self.preco_maximo):
                fora += 1
        return ResultadoExpectation(
            nome="preco_dentro_do_intervalo",
            coluna="preco",
            sucesso=fora == 0 and nao_numerico == 0,
            detalhe=f"fora_do_intervalo={fora} nao_numerico={nao_numerico}",
        )

    def volume_nao_negativo(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """volume é numérico e >= 0."""
        negativos = 0
        nao_numerico = 0
        for linha in linhas:
            valor = linha.get("volume")
            try:
                numero = float(valor)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                nao_numerico += 1
                continue
            if numero < self.volume_minimo:
                negativos += 1
        return ResultadoExpectation(
            nome="volume_nao_negativo",
            coluna="volume",
            sucesso=negativos == 0 and nao_numerico == 0,
            detalhe=f"negativos={negativos} nao_numerico={nao_numerico}",
        )

    def timestamp_nao_nulo(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """timestamp é a base da métrica de freshness — não pode faltar."""
        nulos = sum(1 for linha in linhas if linha.get("timestamp") is None)
        return ResultadoExpectation(
            nome="timestamp_nao_nulo",
            coluna="timestamp",
            sucesso=nulos == 0,
            detalhe=f"nulos={nulos}",
        )

    def uma_linha_por_moeda_e_timestamp(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """O grão do staging é (moeda, timestamp) — não pode haver duplicata."""
        vistos: set[tuple[Any, Any]] = set()
        duplicados = 0
        for linha in linhas:
            chave = (linha.get("moeda"), linha.get("timestamp"))
            if chave in vistos:
                duplicados += 1
            vistos.add(chave)
        return ResultadoExpectation(
            nome="uma_linha_por_moeda_e_timestamp",
            coluna="moeda,timestamp",
            sucesso=duplicados == 0,
            detalhe=f"duplicados={duplicados}",
        )

    # ------------------------------------------------------------------ #
    # Execução
    # ------------------------------------------------------------------ #
    def validar(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> list[ResultadoExpectation]:
        """Roda todas as expectations sobre uma coleção de linhas."""
        materializadas = list(linhas)
        return [expectation(materializadas) for expectation in self.expectations]


def create_mercado_suite() -> SuiteMercado:
    """Fábrica usada por scripts/run_expectations.py e pelos testes."""
    return SuiteMercado()