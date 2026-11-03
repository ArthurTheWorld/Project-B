-- Latencia ponta a ponta em segundos, sem armadilha de fuso.
-- unix_timestamp() interpreta o timestamp no fuso da sessao; os valores gravados
-- estao em UTC, entao a subtracao direta de epoch esconde 3h (America/Sao_Paulo).
-- cast(... as long) usa o epoch real gravado, imune a sessao.
create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');

select
    count(*)                                                                    as linhas,
    round(avg(cast(_ingerido_em as long) - cast(_evento_em as long)), 3)        as latencia_media_s,
    round(percentile(cast(_ingerido_em as long) - cast(_evento_em as long), 0.5), 3)  as latencia_p50_s,
    round(percentile(cast(_ingerido_em as long) - cast(_evento_em as long), 0.95), 3) as latencia_p95_s,
    round(max(cast(_ingerido_em as long) - cast(_evento_em as long)), 3)        as latencia_max_s
from pedidos
where _evento_em is not null and _ingerido_em is not null;