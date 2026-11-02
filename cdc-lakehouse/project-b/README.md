# Projeto B – Confiabilidade de Dados (SLOs, Lineage e Incident Response)

**Objetivo:** Transformar a afirmação "nossos pipelines são confiáveis" em evidência mensurável através de SLOs (Objetivos de Nível de Serviço), rastreamento de lineage completo e um processo de incident response com MT medido.

## Por que este projeto?
- O seu perfil menciona "pipelines confiáveis", mas hoje isso é afirmação, não evidência.
- Este projeto transforma a frase em sistema e conversa com o que você já tem (stack CDC, dbt, Metabase).
- Entregáveis de alto impacto para LinkedIn/portfólio: MTTR reduzido, % de incidentes detectados antes do stakeholder, catálogo de SLOs versionado.

## Escopo (3‑4 semanas, ~10 h/semana)

### 1. Catálogo de SLOs
- Definir, para cada tabela crítica (ex.: `loja.pedidos`, `stg_coingecko__mercado`), metas de:
  - **Frescor** – atualização máxima (ex.: > 95 % dos dados com menos de 1 h).
  - **Volume** – linhas esperadas por lote.
  - **Schema** – mudanças não permitidas sem nova versão.
  - **Distribuição** – percentual de pedidos por status, etc.
- Salvar em `slo_catalog.yaml` versionado.

### 2. Instrumentação de Lineage
- Integrar **OpenLineage** ou **Marquez** ao job Spark/btf para capturar lineage completa de origem → Bronze → Silver → Gold.
- Criar scripts que extraem os metadados de linha e os inserem no catálogo.

### 3. Testes de anomalia (Great Expectations / Elementary)
- Adicionar testes de: null‑rate, distinct‑rate, monotonicidade, rangos de valores.
- Os resultados alimentam os SLOs e aparecem no painel de saúde.

### 4. Painel de saúde da plataforma (Metabase / Grafana)
- Exibir status "✅/❌" por SLO.
- Mostrar tendência de falhas nos últimos 7 dias.
- Links rápidos para runbooks.

### 5. Runbooks
- Documentos curtos (1‑2 páginas) para os 3 tipos de falha mais comuns:
  - *Latência alta* – verificar lag do consumidor, reiniciar Spark job.
  - *Volume inesperado* – checar se houve pico de inserções no source.
  - *Schema change* – validar auto‑merge do Delta e ajustar dbt models.

### 6. Incidentes simulados
- Disparar artificialmente uma falha (ex.: parar o Spark, inserir dados inválidos) e medir:
  - **Tempo de detecção** (quanto tempo até o painel sinalizar).
  - **Tempo de resolução** (MTTR).
- Documentar resultados no README final.

### 7. Métricas finais no README
- **MTTR antes** (ex.: 4 h) → **depois** (ex.: 20 min).
- **% de incidentes detectados antes do stakeholder** (ex.: 90 %).
- Link para o painel e para o catálogo de SLOs.

### 8. Incidentes simulados e MTTR
- Disparar artificialmente uma falha (ex.: pausar Spark, injetar dados extras, alterar schema) e medir:
  - **Tempo de detecção** (quanto tempo até o painel sinalizar / tracker registrar).
  - **Tempo de resolução** (MTTR via `scripts/mttr_tracker.py`).
- Registrar resultados no `mttr_log.json` e no `READMEfinal.md`.
- Scripts auxiliares: `simular_latencia_alta.py`, `simular_volume_inesperado.py`, `simular_schema_change.py`.

### 9. Decisão final: próximo foco
Após a coleta de métricas de incidentes e MTTR, decidir:
- **Iterar Project A** (CDC platform) – aprimorar Debezium, Spark job, escalabilidade.
- **Explorar outro tópico** de engenharia de dados (ex.: streaming com Flink, data warehouse otimizado, governance automatizada).

### 10. Estrutura de pastas (planejada)

```
project-b/
├─ README.md            # Este arquivo
├─ slo_catalog.yaml     # Catálogo de SLOs versionado
├─ expectations/       # Great Expectations expectations suite
├─ lineage/            # Scripts de extração de lineage (OpenLineage/Marquez)
├─ runbooks/           # Markdown com runbooks de falha
├─ dashboards/         # Configurações do Metabase / Grafana
├─ scripts/            # Scripts de incidentes simulados e automação
└─ READMEfinal.md      # README final com métricas e learnings
```

## Próximos passos imediatos ( primeira semana )
1. **Definir KPIs e SLOs** – sentar com os donos das tabelas e acordar metas realistas.
2. **Criar `slo_catalog.yaml`** – modelo inicial com as colunas: `table_name`, `freshness_target`, `volume_target`, `schema_target`, `distribution_target`.
3. **Integrar OpenLineage** ao job Spark existente (`spark/job.py`) – adicionar o hook `openlineage.spark` e garantir que os eventos sejam emitidos.
4. **Criar primeira expectation** com Great Expectations – ex.: `expect_column_values_to_be_between` para `preço` no `stg_coingecko__mercado`.
5. **Construir painel inicial** no Metabase – uma página "Saúde da Plataforma" com indicadores de frescor e volume.

---

*Este projeto fechará a maior lacuna do seu perfil ("Confiabilidade (SLO de dados, lineage, incidente, MTTR)") e trará números concretos para seu portfólio e LinkedIn.*