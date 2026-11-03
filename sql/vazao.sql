-- Vazao real: linhas por minuto numa janela recente, com o gerador ativo.
-- Tambem calcula eventos/segundo a partir do intervalo entre lotes.
create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');

select
    date_trunc('minute', _ingerido_em) as minuto,
    count(*)                          as linhas,
    count(distinct pedido_id)         as pedidos_distintos
from pedidos
where _ingerido_em >= current_timestamp() - interval '20 minutes'
group by 1
order by 1 desc;