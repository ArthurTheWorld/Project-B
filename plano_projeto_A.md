# Plano de Projeto – Plataforma de Dados em Tempo Real com CDC (Projeto A)

## 1. Visão geral
- **Objetivo:** Construir e operar um pipeline de Change Data Capture (CDC) de verdade que entrega dados analíticos em minutos, garantindo exactly‑once, idempotência, evolução de schema e suporte a deletes.
- **Valor para o negócio:** Redução de latência de dados de horas/dias para minutos, permitindo decisões em tempo real e evitando reprocessamento de grandes volumes.

## 2. Escopo
### In‑scope
- Deploy de stack Docker‑Compose (Postgres, Debezium, Kafka, Schema Registry, Spark, MinIO/Delta, Metabase).
- Simulador de carga (≈ 5 k eventos/s) usando pgbench ou script próprio.
- Job Spark que consome o tópico Kafka, faz MERGE idempotente (pk + lsn) e grava no Delta Lake.
- Suporte a schema evolution (adicionar coluna, renomear, mudar tipo) sem downtime.
- Suporte a deletes (tombstones) e handling de late‑arriving events.
- Dashboard de métricas em tempo real (latência p50/p95, throughput, lag do consumidor, custo/hora).
- Documentação completa (README, ADRs, scripts de start/stop, teste de replay, dashboard).

### Out‑of‑scope
- Integração com sistemas de produção externos (ex.: APIs de terceiros) – foco apenas em banco simulado.
- Deploy em cluster Kubernetes – usamos Docker Desktop.
- Otimização avançada de Spark (ex.: cluster mode) – modo local suficiente para demonstração.

## 3. Arquitetura (Mermaid)

```mermaid
flowchart LR
    PG[(Postgres<br/>OLTP<br/>WAL lógico)] -->|pgoutput| DBZ[Debezium<br/>Kafka Connect]
    DBZ --> K[(Kafka<br/>KRaft)] 
    K -->|Structured Streaming| SP[Spark Structured Streaming]
    SP -->|MERGE idempotente| DL[(Delta Lake<br/>bronze)] 
    DL --> dbt[dbt (silver → gold)] 
    dbt --> BI[Metabase / Superset]
    GEN[(Gerador de carga)] -->|INSERT/UPDATE/DELETE| PG
```

## 4. Cronograma (90 dias – foco nas primeiras 6 semanas)

| Semana | Atividade principal | Entregável |
|--------|---------------------|------------|
| 1‑2 | Provisionar Docker Desktop, criar `docker‑compose.yml` (já existe) e subir stack | Stack rodando (healthcheck green) |
| 3 | Configurar conector Debezium (slot, publication) e validar produção de eventos no Kafka | Tópico `loja.pedidos` com eventos visíveis |
| 4 | Desenvolver/ajustar job Spark (`job.py`) – consumir tópico, fazer MERGE idempotente | Job rodando, latência medida |
| 5 | Testes de exactly‑once: replay completo (24 h) e verificação de zero duplicatas | Relatório de teste + dashboard de latência |
| 6 | Implementar schema evolution (adicionar coluna) e validar pipeline sem downtime | ADR atualizado, demonstração video |
| 6‑7 | Dashboard de métricas (Grafana ou Metabase) e alertas (p95 latency > 2 s) | Dashboard publicado |
| 7 | Documentação final (README, ADRs, scripts) e gravação de GIF 60 s | Repo público, README completo |

## 5. Métricas de Sucesso (KPIs)

| KPI | Meta |
|-----|------|
| Latência ponta‑a‑ponta (p95) | ≤ 2 s |
| Vazão sustentada | ≥ 5 000 eventos/s |
| Zero duplicatas após replay de 24 h | 100 % (nenhum registro duplicado) |
| Custo operacional (CPU + RAM) | ≤ US$ 0,30 / hora (instância t3.medium) |
| Sucesso no schema evolution | 100 % das alterações (coluna, rename, type) concluídas sem reiniciar containers |

## 5.1. Métricas de Exactly‑Once
- **Chave de merge:** `(pedido_id, lsn)` – garante que, mesmo que o mesmo pedido seja processado múltiplas vezes, o estado final seja o mesmo.  
- **Teste de replay:** apagar checkpoint, reiniciar job, comparar contagem de linhas no Delta antes e depois → 1:1.

## 6. Tarefas detalhadas

1. **Ambiente**  
   - Iniciar Docker Desktop.  
   - `cd cdc-lakehouse && docker compose up -d --build`.  
   - Verificar healthchecks (`docker compose ps`).  

2. **Conector Debezium**  
   - Confirmar `slot.name = loja_slot` e `publication.name = loja_pub`.  
   - Executar `curl http://localhost:8083/connectors/loja-pedidos/status` → `RUNNING`.  

3. **Simulador de carga**  
   - Utilizar `pgbench` ou script `scripts/demo-evolucao.ps1` para gerar 5 k evt/s.  
   - Verificar contagem no Postgres e no Delta (deve bater).  

4. **Job Spark**  
   - Implementar/ajustar `job.py` para:  
     - Ler do Kafka (`loja.loja.pedidos`).  
     - Aplicar `foreachBatch` com `MERGE` usando `pedido_id` + `lsn`.  
     - Escrever no Delta (`/data/delta/pedidos`).  
   - Configurar `checkpointLocation` para habilitar exactly‑once.  

5. **Teste de exactly‑once**  
   - Parar job, excluir checkpoint, iniciar novamente.  
   - Executar `SELECT count(*) FROM delta.pedidos` antes e depois → mesmo número.  

6. **Schema evolution**  
   - `ALTER TABLE loja.pedidos ADD COLUMN cupom TEXT;` (via psql).  
   - Atualizar alguns registros (`UPDATE loja.pedidos SET cupom='BLACK50' WHERE ...`).  
   - Verificar que a coluna chega ao Delta e que o dbt a inclui nos modelos.  

7. **Dashboard**  
   - Criar painel no Metabase/Grafana com:  
     - Latência (tempo entre write no Postgres e aparecer no Delta).  
     - Throughput (events/s).  
     - Lag do consumidor (Kafka offset lag).  
   - Configurar alerta Slack/email para p95 latency > 2 s.  

8. **Documentação**  
   - README.md com:  
     - Visão geral, diagrama, instruções de start/stop.  
     - Lista de ADRs (docs/decisoes.md).  
   - Scripts `scripts/start.sh`, `scripts/stop.sh`, `scripts/demo-replay.ps1`.  
   - GIF/Video 60 s mostrando o fluxo completo.  

## 7. Riscos & Mitigações

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| Docker Desktop não disponível | Impede deploy da stack | Usuário deve iniciar Docker Desktop; caso contrário, usar VM com Docker. |
| Falha no conector Debezium (slot não criado) | Dados não chegam ao Kafka | Verificar logs do Debezium; garantir `publication.autocreate.mode=filtered`. |
| Schema change quebra o MERGE | Pipeline falha | Testar schema evolution antes de aplicar em produção; usar `spark.databricks.delta.schema.autoMerge.enabled=true`. |
| Custo de infra (CPU/RAM) acima do orçamento | Viabilidade financeira | Usar instância t3.medium (2 vCPU, 4 GB) e monitorar consumo; ajustar `spark.driver-memory`. |
| Alert fatigue | Ignorar alertas críticos | Agrupar alertas por hora, silenciar durante manutenção, usar thresholds baseados em p95. |

## 8. Critérios de Aceitação

- Stack está totalmente funcional (todos os containers saudáveis).  
- Job Spark processa 5 k evt/s com latência p95 ≤ 2 s.  
- Replay de 24 h não cria duplicatas (contagem idêntica).  
- Schema evolution concluída com sucesso em até 3 tentativas.  
- Dashboard exibindo métricas em tempo real e enviando alertas corretos.  
- Documentação completa e versionada; repositório público com README, ADRs, scripts e GIF.

## 9. Próximos passos imediatos

1. Usuário inicia Docker Desktop.  
2. Executa `docker compose up -d --build` no diretório `cdc-lakehouse`.  
3. Aguarda confirmação de que os containers estão “healthy”.  
4. Fornece feedback se houver algum passo bloqueado (ex.: portas, recursos de memória).  

Com essas informações, o plano está pronto para ser executado. Estou à disposição para ajustes ou esclarecimentos.