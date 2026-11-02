-- Coleta única para o monitor: UM spark-sql para TODAS as métricas.
-- Cada bloco começa com um SELECT marcador que o parser Python reconhece.

create or replace temporary view pedidos using delta
options (path '/data/delta/pedidos');

select '@@TOTAL@@' as marcador;
select count(*) as linhas from pedidos;

select '@@LATENCIA@@' as marcador;
select
    round(avg(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2)      as media,
    round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.5), 2)  as p50,
    round(percentile(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em), 0.95), 2) as p95,
    round(max(unix_timestamp(_ingerido_em) - unix_timestamp(_evento_em)), 2)      as maxima
from pedidos
where _evento_em is not null and _ingerido_em is not null;

select '@@OPERACAO@@' as marcador;
select _op, count(*) as total from pedidos group by _op order by total desc;

select '@@STATUS@@' as marcador;
select status, count(*) as total from pedidos group by status order by total desc;

select '@@SERIE@@' as marcador;
select date_trunc('minute', _ingerido_em) as minuto, count(*) as linhas
from pedidos
where _ingerido_em >= current_timestamp() - interval '15 minutes'
group by 1 order by 1;
