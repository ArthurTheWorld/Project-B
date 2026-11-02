"""Servidor do monitor: expõe as métricas em JSON e serve o painel.

Escolha de projeto: um `http.server` da stdlib em vez de FastAPI/Streamlit.
O painel é uma página estática que busca `/api/metricas` a cada 15 s — não
precisa de framework, template engine nem container extra.

A coleta é pesada (sobe um Spark por consulta), então roda em thread separada
com cache: o endpoint nunca dispara duas coletas ao mesmo tempo.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from monitor.coleta import coletar, coletar_origem

log = logging.getLogger(__name__)

PORTA = 8100
INTERVALO_CACHE_S = 15
DIRETORIO = Path(__file__).resolve().parent

_lock = threading.Lock()
_cache: dict = {"dados": None, "em": 0.0, "coletando": False}


def _coletar_em_background() -> None:
    """Coleta fora do request handler, para não bloquear o navegador."""
    with _lock:
        if _cache["coletando"]:
            return
        _cache["coletando"] = True
    try:
        metricas = coletar(coletar_origem())
        with _lock:
            _cache["dados"] = metricas.to_dict()
            _cache["em"] = time.time()
    except Exception as exc:  # noqa: BLE001
        log.exception("erro na coleta em background")
        with _lock:
            _cache["dados"] = {"erro": str(exc)[:300], "coletado_em": None}
            _cache["em"] = time.time()
    finally:
        with _lock:
            _cache["coletando"] = False


def metricas_atuais() -> dict:
    """Devolve o cache; se vencido, dispara coleta e espera."""
    with _lock:
        dados = _cache["dados"]
        idade = time.time() - _cache["em"]

    if dados is None or idade > INTERVALO_CACHE_S:
        _coletar_em_background()
        # espera a coleta iniciar sem travar indefinidamente
        for _ in range(60):
            with _lock:
                if _cache["dados"] is not None and not _cache["coletando"]:
                    dados = _cache["dados"]
                    break
            time.sleep(0.5)

    with _lock:
        return _cache["dados"] or {"erro": "coleta ainda não concluiu"}


class Handler(BaseHTTPRequestHandler):
    def _responder(self, corpo: bytes, tipo: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(corpo)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(corpo)

    def do_GET(self) -> None:  # noqa: N802 - assinatura da stdlib
        if self.path.startswith("/api/metricas"):
            corpo = json.dumps(metricas_atuais(), ensure_ascii=False).encode("utf-8")
            self._responder(corpo, "application/json; charset=utf-8")
            return

        if self.path in ("/", "/index.html"):
            pagina = (DIRETORIO / "index.html").read_bytes()
            self._responder(pagina, "text/html; charset=utf-8")
            return

        if self.path.startswith("/api/coletar"):
            threading.Thread(target=_coletar_em_background, daemon=True).start()
            self._responder(b'{"ok":true}', "application/json")
            return

        self._responder(b"nao encontrado", "text/plain; charset=utf-8", status=404)

    def log_message(self, formato: str, *args) -> None:
        pass  # silencia o log de cada request


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(f"monitor em http://localhost:{PORTA}")
    threading.Thread(target=_coletar_em_background, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORTA), Handler).serve_forever()


if __name__ == "__main__":
    main()
