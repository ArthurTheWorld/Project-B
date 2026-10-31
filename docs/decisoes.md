# Decisões de arquitetura (ADRs)

## ADR-001 — CDC por WAL lógico, não polling

**Contexto.** A alternativa comum é uma DAG que consulta `updated_at > ultima_execucao`.

**Decisão.** Debezium lendo o WAL lógico do Postgres (`pgoutput`).

**Consequências.**

- (+) Captura `DELETE` (polling nunca vê)
- (+) Não perde estado intermediário: toda transição vira evento
- (+) Não pressiona o banco com queries
- (−) Exige `wal_level=logical`, slot de replicação e um serviço a mais
- (−) Slot abandonado acumula WAL e pode encher o disco — precisa monitorar

---

## ADR-002 — `REPLICA IDENTITY FULL`

**Contexto.** Por padrão, o Postgres só envia a chave primária no `before` de um
update/delete.

**Decisão.** `alter table loja.pedidos replica identity full`.

**Consequências.**

- (+) `before` traz todas as colunas, permitindo auditoria e merge com segurança
- (−) Mais dados no WAL; em tabelas grandes, custo relevante

---

## ADR-003 — JSON no primeiro momento (Avro no roadmap)

**Contexto.** Avro + Schema Registry é o padrão de mercado. Também é a parte mais
frágil de operar: o Spark não desserializa Avro nativamente com registry — é
preciso buscar o schema por mensagem.

**Decisão.** JSON com `schemas.enable=false` no Debezium.

**Consequências.**

- (+) Pipeline menor e mais fácil de rodar/depurar
- (+) O objetivo do projeto (merge idempotente e evolução) não depende do formato
- (−) Sem contrato binário nem compatibilidade forçada
- (−) Payload maior no tópico

Avro é o primeiro item do roadmap, justamente por ser o que falta para o
projeto ficar mais próximo de produção.

---

## ADR-004 — MERGE por chave em vez de append

**Contexto.** O caminho mais simples seria gravar o log de eventos no lake
(append-only) e materializar a visão depois.

**Decisão.** Aplicar cada lote com `MERGE` por `pedido_id`.

**Consequências.**

- (+) O lake reflete o estado atual — consulta simples, sem window function
- (+) Reprocessar é inofensivo (idempotente por chave)
- (−) Perde-se o histórico de mudanças dentro do lake (mitigado pelo time travel
  do Delta)
- (−) MERGE custa mais que append

---

## ADR-005 — `foreachBatch` em vez de `writeStream` direto

**Contexto.** `df.writeStream.format("delta").start()` só faz append.

**Decisão.** `foreachBatch`, que recebe cada micro-lote como DataFrame e permite
rodar SQL (o MERGE) e tratar deletes.

**Consequências.**

- (+) Upsert e delete no mesmo comando
- (+) Ponto natural para aplicar regras por lote
- (−) O `foreachBatch` roda no driver; lógica pesada ali vira gargalo

---

## ADR-006 — Delta Lake

**Contexto.** Parquet puro não tem transação, nem merge, nem evolução de schema.

**Decisão.** Delta 3.2 com `schema.autoMerge.enabled=true`.

**Consequências.**

- (+) ACID, time travel e `MERGE` nativo
- (+) Schema evolution sem parar o job
- (−) Dependência de jars e de um formato com governança própria
- (−) Small files exigem compactação periódica (`OPTIMIZE`) — não implementado

---

## ADR-007 — Um tópico por tabela

**Contexto.** O Debezium pode publicar todas as tabelas num tópico só.

**Decisão.** `table.include.list=loja.pedidos` e um tópico por tabela.

**Consequências.**

- (+) Consumidor simples, sem roteamento por payload
- (+) Paralelismo e retenção independentes
- (−) Muitas tabelas = muitos tópicos e muitos jobs (mitigável com Connect sink)

---

## ADR-008 — Gerador de carga próprio

**Contexto.** Sem carga, o pipeline fica vazio e não demonstra nada.

**Decisão.** `generator/carga.py`, que insere, atualiza e cancela pedidos em
loop, com taxas configuráveis.

**Consequências.**

- (+) Demonstração reprodutível com `docker compose up`
- (+) Permite simular cenários (parar a carga, alterar schema)
- (−) Código que não é "produto"; aceitável no contexto de portfólio
