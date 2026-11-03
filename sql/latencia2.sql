-- Latencia ponta a ponta: diferenca em segundos entre o instante do evento no
-- Postgres e o instante de processamento do lote no Spark.
-- cast(... as long) usa o epoch real gravado (UTC), imune ao fuso da sessao.
create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');

select
    count(*)                                                                          as linhas,
    round(avg(cast(cast(_ingerido_em as long) as double) - cast(cast(_evento_em as long) as double)), 3) as latencia_media_s,
    round(percentile(cast(cast(_ingerido_em as long) as double) - cast(cast(_evento_em as long) as double), 0.5), 3) as latencia_p50_s,
    round(percentile(cast(cast(_ingerido_em as long) as double) - cast(cast(_evento_em as long) as double), 0.95), 3) as latencia_p95_s,
    round(max(cast(cast(_ingerido_em as long) as double) - cast(cast(_evento_em as long) as double)), 3) as latencia_max_s,
    round(min(cast(cast(_ingerido_em as long) as double) - cast(cast(_evento_em as long) as double)), 3) as latencia_min_s
from pedidos
where _evento_em is not null and _ingerido_em is not null;