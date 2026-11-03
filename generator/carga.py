"""Gerador de carga: simula a operação de um e-commerce no OLTP.

Insere, atualiza e cancela pedidos continuamente. É a fonte de eventos do CDC:
o Debezium não "consulta" nada — ele lê o WAL, então cada INSERT/UPDATE/DELETE
aqui vira um evento no Kafka em segundos.
"""

from __future__ import annotations

import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("carga")

INTERVALO_MS = int(os.getenv("INTERVALO_MS", "400"))
TAXA_UPDATE = float(os.getenv("TAXA_UPDATE", "0.35"))
TAXA_DELETE = float(os.getenv("TAXA_DELETE", "0.02"))

CANAIS = ["app", "web", "marketplace", "loja_fisica"]
STATUS = ["novo", "pago", "separacao", "enviado", "entregue", "cancelado"]

parar = False


def _sinal(signum, frame):
    global parar
    parar = True
    log.info("encerrando gerador...")


def conectar():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "loja"),
        user=os.getenv("PGUSER", "loja"),
        password=os.getenv("PGPASSWORD", "loja"),
    )


def proximo_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("select coalesce(max(pedido_id), 0) + 1 from loja.pedidos")
        return int(cur.fetchone()[0])


def inserir(conn, pedido_id: int, rnd: random.Random) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into loja.pedidos (pedido_id, cliente_id, status, valor, canal, criado_em, atualizado_em)
            values (%s, %s, 'novo', %s, %s, now(), now())
            """,
            (
                pedido_id,
                rnd.randint(1, 5000),
                round(rnd.uniform(25, 900), 2),
                rnd.choice(CANAIS),
            ),
        )
    conn.commit()


def atualizar(conn, rnd: random.Random) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update loja.pedidos
               set status = %s,
                   atualizado_em = now()
             where pedido_id = (
                 select pedido_id from loja.pedidos
                  where status not in ('entregue', 'cancelado')
                  order by random() limit 1
             )
            """,
            (rnd.choice(STATUS),),
        )
    conn.commit()


def deletar(conn, rnd: random.Random) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from loja.pedidos
             where pedido_id = (
                 select pedido_id from loja.pedidos
                  where status = 'cancelado'
                  order by random() limit 1
             )
            """
        )
    conn.commit()


def main() -> int:
    signal.signal(signal.SIGTERM, _sinal)
    signal.signal(signal.SIGINT, _sinal)

    rnd = random.Random()
    conn = conectar()
    pedido_id = proximo_id(conn)

    log.info("gerando eventos a cada %dms (taxa update=%.2f delete=%.2f)", INTERVALO_MS, TAXA_UPDATE, TAXA_DELETE)

    while not parar:
        sorteio = rnd.random()
        try:
            if sorteio < TAXA_DELETE:
                deletar(conn, rnd)
            elif sorteio < TAXA_DELETE + TAXA_UPDATE:
                atualizar(conn, rnd)
            else:
                inserir(conn, pedido_id, rnd)
                pedido_id += 1
        except psycopg2.Error as exc:
            log.warning("erro no banco (%s); reconectando", exc)
            conn.rollback()
            time.sleep(2)

        time.sleep(INTERVALO_MS / 1000)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
