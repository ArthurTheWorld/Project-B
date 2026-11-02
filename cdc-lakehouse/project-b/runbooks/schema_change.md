# Runbook: Mudança de Schema

## Sintomas
- O campo novo aparece no Delta table sem aviso prévio.
- Os expectations falham porque a coluna esperada não existe.
- O `slo_catalog.yaml` marca `allow_add_column: false` e o schema change bloqueia o merge.

## Passos de Mitigação

1. **Identificar a coluna nova** – rodar `python -c "from delta.tables import *; ..."` ou usar `DESCRIBE HISTORY` no Delta log.
2. **Se a coluna for esperada** (ex.: nova feature de produto):
   - Atualizar o `slo_catalog.yaml` para `allow_add_column: true` (se o Delta suportar auto‑merge).
   - Rodar `spark.sql("ALTER TABLE delta.loja_pedidos ADD COLUMNS (nova_coluna STRING)")`.
3. **Se a coluna for inesperada**:
   - Parar o Spark job: `./scripts/stop.ps1`.
   - Revisar o source (Kafka schema, dbt model) para entender a origem.
   - Se for uma mudança breaking, criar nova versão do catálogo (`slo_catalog.yaml` versão 2.0) e atualizar os expectations (`pedidos_expectations.py` / `mercado_expectations.py`).
4. **Reiniciar o Spark job** e validar que as expectations passam.
5. **Registrar a mudança** no runbook de incidentes e no `READMEfinal.md`.

## Checklist de Pós‑Resolução
- [ ] Schema do Delta está documentado no `slo_catalog.yaml`.
- [ ] Expectations válidas (rodam sem erros de coluna missing).
- [ ] Painel Metabase está verde para os SLOs afetados.
- [ ] Mudança registrada no histórico de runbooks.