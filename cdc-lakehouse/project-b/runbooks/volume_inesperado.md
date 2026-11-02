# Runbook: Volume Inesperado

## Sintomas
- Indicador de volume do SLO `loja.pedidos` está vermelho (linhas/lote < 100 ou > meta).
- Painel de saúde mostra "❌" ao lado de `volume_target`.
- Aumento repentino de mensagens no topic Kafka.

## Passos de Mitigação

1. **Confirmar o pico** – usar o Kafka Monitor (`monitor/servidor.py`) para ver o volume de mensagens nos últimos 5 min.
2. **Verificar source system** – há um lançamento de campanha, novo produto, ou erro de ingestão?
3. **Ajustar partitions** (se necessário):
   ```bash
   spark.conf.set("spark.sql.shuffle.partitions", "8")
   ```
4. **Reiniciar o Spark job** para que o novo número de partitions entre em efeito.
5. **Validar SLO** – após 3 batches, confirme que o volume está dentro do `min_rows_per_batch`.
6. **Se o volume persistir acima do target**, considerar aumentar o `min_rows_per_batch` no `slo_catalog.yaml`.

## Checklist de Pós‑Resolução
- [ ] Volume por batch dentro do target por 3 cycles consecutivos.
- [ ] Nenhum error de "min_rows" nos logs dos últimos 20 min.
- [ ] Documentar a causa raiz no incidente.
- [ ] Atualizar `slo_catalog.yaml` se o target for ajustado.