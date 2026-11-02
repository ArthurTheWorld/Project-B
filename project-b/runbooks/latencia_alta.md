# Runbook: Latência Alta

## Sintomas
- Indicador de frescor do SLO `loja.pedidos` está vermelho (> 60 min de atraso).
- Painel de saúde mostra "❌" ao lado de `freshness_target`.
- Logs do Spark indicam lag crescente do consumidor Kafka.

## Passos de Mitigação

1. **Verificar lag do consumidor**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
     --group-id spark-consumer \
     --describe
   ```
2. **Reiniciar o Spark job** (se o lag for irreversível):
   ```bash
   ./scripts/stop.ps1
   ./scripts/start.ps1
   ```
3. **Checar métricas do Kafka** – topics under-replicated, broker load.
4. **Validar SLO após reinício** – aguarde 2 batches (10 s cada) e confira o painel Metabase.
5. **Registrar o incidente** no runbook de incidentes (ver `README.md` → "Incidentes simulados").

## Checklist de Pós‑Resolução
- [ ] Frescor dentro do target (< 60 min) por 2 cycles consecutivos.
- [ ] Nenhum erro de "max.age" nos logs dos últimos 15 min.
- [ ] Atualizar métrica de MTTR no README final.