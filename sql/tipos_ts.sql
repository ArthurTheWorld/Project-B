-- Inspeciona os tipos e valores crus das colunas de tempo no Delta.
create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');

select _evento_em, _ingerido_em, _op
from pedidos
where _evento_em is not null
order by pedido_id desc
limit 5;