-- Consultas de verificação do pipeline CDC.
-- Rode com: docker compose exec -T spark spark-sql ... -f /opt/sql/verificacao.sql

create or replace temporary view pedidos using delta
options (path '/data/delta/pedidos');

-- 1) Estado atual: deve bater com o count(*) de loja.pedidos no Postgres
select count(*) as pedidos_no_lake from pedidos;

-- 2) Distribuição por status
select status, count(*) as total from pedidos group by status order by total desc;

-- 3) Quantos registros foram removidos por CDC (auditoria do delete)
select _op, count(*) as total from pedidos group by _op order by total desc;

-- 4) Histórico de versões da tabela (time travel do Delta)
select version, timestamp, operation from (describe history pedidos) limit 10;
