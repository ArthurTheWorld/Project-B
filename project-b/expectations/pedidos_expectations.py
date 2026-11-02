"""Suíte de expectations para loja.pedidos (Projeto B).

Este arquivo é Python executável de verdade — não um documento Markdown.
Os nomes de colunas seguem o schema real definido em
``postgres/init/01_schema.sql`` e materializado em Delta por ``spark/job.py``:

    pedido_id, cliente_id, status, valor, canal, criado_em, atualizado_em
    + colunas técnicas: _op, _lsn, _evento_em, _ingerido_em

O domínio de ``status`` é o do gerador de carga (``generator/carga.py``):
``novo``, ``pago``, ``enviado``, ``entregue``, ``cancelado``.

Por que não usar great_expectations aqui: o pacote não está instalado no
ambiente (nem no .venv) e o projeto roda em CI sem Spark. As expectations
abaixo são declarativas e verificáveis com pandas puro — o mesmo contrato,
sem dependência pesada. ``scripts/run_expectations.py`` executa este módulo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping


# Schema real de loja.pedidos (fonte: postgres/init/01_schema.sql + spark/job.py)
COLUNAS_OBRIGATORIAS: tuple[str, ...] = (
    "pedido_id",
    "cliente_id",
    "status",
    "valor",
    "canal",
    "criado_em",
    "atualizado_em",
)

COLUNAS_TECNICAS: tuple[str, ...] = ("_op", "_lsn", "_evento_em", "_ingerido_em")

STATUS_VALIDOS: tuple[str, ...] = (
    "novo",
    "pago",
    "enviado",
    "entregue",
    "cancelado",
)

CANAIS_VALIDOS: tuple[str, ...] = ("app", "web", "loja", "telefone")

# Limite de valor coerente com numeric(12,2) e o gerador de carga.
VALOR_MINIMO = 0.01
VALOR_MAXIMO = 1_000_000.00


@dataclass(frozen=True)
class ResultadoExpectation:
    """Resultado de uma expectation individual."""

    nome: str
    coluna: str | None
    sucesso: bool
    detalhe: str = ""


@dataclass
class SuitePedidos:
    """Suíte de expectativas para a tabela loja.pedidos."""

    nome: str = "pedidos_expectations"
    tabela: str = "loja.pedidos"
    colunas_obrigatorias: tuple[str, ...] = COLUNAS_OBRIGATORIAS
    status_validos: tuple[str, ...] = STATUS_VALIDOS
    canais_validos: tuple[str, ...] = CANAIS_VALIDOS
    valor_minimo: float = VALOR_MINIMO
    valor_maximo: float = VALOR_MAXIMO
    expectations: list[Callable[[Iterable[Mapping[str, Any]]], ResultadoExpectation]] = field(
        default_factory=list
    )

    def __post_init__(self) -> None:
        if not self.expectations:
            self.expectations = [
                self.colunas_esperadas_presentes,
                self.pedido_id_nao_nulo_e_unico,
                self.status_no_dominio,
                self.valor_positivo_e_dentro_do_limite,
                self.canal_no_dominio,
                self.criado_em_nao_nulo,
                self.atualizado_em_nao_anterior_a_criado_em,
            ]

    # ------------------------------------------------------------------ #
    # Expectations
    # ------------------------------------------------------------------ #
    def colunas_esperadas_presentes(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """Toda linha tem as colunas obrigatórias e as técnicas."""
        esperadas = set(self.colunas_obrigatorias) | set(COLUNAS_TECNICAS)
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

    def pedido_id_nao_nulo_e_unico(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """pedido_id nunca é nulo e não se repete (é a chave do MERGE)."""
        vistos: set[Any] = set()
        duplicados = 0
        nulos = 0
        for linha in linhas:
            valor = linha.get("pedido_id")
            if valor is None:
                nulos += 1
                continue
            if valor in vistos:
                duplicados += 1
            vistos.add(valor)
        return ResultadoExpectation(
            nome="pedido_id_nao_nulo_e_unico",
            coluna="pedido_id",
            sucesso=nulos == 0 and duplicados == 0,
            detalhe=f"nulos={nulos} duplicados={duplicados}",
        )

    def status_no_dominio(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """status pertence ao domínio em português do gerador de carga."""
        invalidos: dict[Any, int] = {}
        for linha in linhas:
            valor = linha.get("status")
            if valor not in self.status_validos:
                invalidos[valor] = invalidos.get(valor, 0) + 1
        return ResultadoExpectation(
            nome="status_no_dominio",
            coluna="status",
            sucesso=not invalidos,
            detalhe="" if not invalidos else f"fora do domínio: {invalidos}",
        )

    def valor_positivo_e_dentro_do_limite(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """valor é numérico, positivo e cabe em numeric(12,2)."""
        fora = 0
        nao_numerico = 0
        for linha in linhas:
            valor = linha.get("valor")
            try:
                numero = float(valor)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                nao_numerico += 1
                continue
            if not (self.valor_minimo <= numero <= self.valor_maximo):
                fora += 1
        return ResultadoExpectation(
            nome="valor_positivo_e_dentro_do_limite",
            coluna="valor",
            sucesso=fora == 0 and nao_numerico == 0,
            detalhe=f"fora_do_intervalo={fora} nao_numerico={nao_numerico}",
        )

    def canal_no_dominio(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """canal pertence ao conjunto de canais conhecidos."""
        invalidos: dict[Any, int] = {}
        for linha in linhas:
            valor = linha.get("canal")
            if valor not in self.canais_validos:
                invalidos[valor] = invalidos.get(valor, 0) + 1
        return ResultadoExpectation(
            nome="canal_no_dominio",
            coluna="canal",
            sucesso=not invalidos,
            detalhe="" if not invalidos else f"fora do domínio: {invalidos}",
        )

    def criado_em_nao_nulo(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """criado_em é a âncora da latência (via _evento_em) — não pode faltar."""
        nulos = sum(1 for linha in linhas if linha.get("criado_em") is None)
        return ResultadoExpectation(
            nome="criado_em_nao_nulo",
            coluna="criado_em",
            sucesso=nulos == 0,
            detalhe=f"nulos={nulos}",
        )

    def atualizado_em_nao_anterior_a_criado_em(
        self, linhas: Iterable[Mapping[str, Any]]
    ) -> ResultadoExpectation:
        """atualizado_em >= criado_em (comparação textual ISO-8601)."""
        violacoes = 0
        for linha in linhas:
            criado = linha.get("criado_em")
            atualizado = linha.get("atualizado_em")
            if criado is None or atualizado is None:
                continue
            if str(atualizado) < str(criado):
                violacoes += 1
        return ResultadoExpectation(
            nome="atualizado_em_nao_anterior_a_criado_em",
            coluna="atualizado_em",
            sucesso=violacoes == 0,
            detalhe=f"violacoes={violacoes}",
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



def create_pedidos_suite() -> SuitePedidos:
    """Fábrica usada por scripts/run_expectations.py e pelos testes."""
    return SuitePedidos()
