-- Métricas do pipeline CDC: latência, vazão e distribuição de operações.
-- Usado pelo monitor (monitor/coleta.py) e executável à mão via spark-sql.

create or replace temporary view pedidos using delta
options (path '/data/delta/pedidos');

-- 1) Latência ponta a ponta: quando a transação ocorreu no Postgres
--    menos quando a linha foi efetivamente gravada no lake.
select
    round(avg(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2)      as latencia_media_s,
    round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.5), 2)  as latencia_p50_s,
    round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.95), 2) as latencia_p95_s,
    round(max(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2)      as latencia_max_s,
    count(*)                                                                       as linhas
from pedidos
where _evento_em is not null and _ingerido_em is not null;

-- 2) Vazão: quantas linhas foram ingeridas por minuto (últimos 10 minutos)
select
    date_trunc('minute', _ingerido_em) as minuto,
    count(*)                            as linhas,
    count(distinct pedido_id)           as pedidos_distintos,
    sum(case when _op = 'd' then 1 else 0 end) as deletes
from pedidos
where _ingerido_em >= current_timestamp() - interval '10 minutes'
group by 1
order by 1 desc;

-- 3) Distribuição por operação (c=create, u=update, r=read, d=delete)
select _op, count(*) as total
from pedidos
group by _op
order by total desc;

-- 4) Estado atual (mesma visão do dashboard de negócio)
select status, count(*) as total
from pedidos
group by status
order by total desc;
