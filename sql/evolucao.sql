-- Verificação de evolução de schema.
-- Depois de rodar: alter table loja.pedidos add column cupom text;

create or replace temporary view pedidos using delta
options (path '/data/delta/pedidos');

-- A coluna nova deve aparecer no Delta sem nenhum restart do job
describe pedidos;

select count(*) as total,
       count(cupom) as com_cupom,
       round(100.0 * count(cupom) / count(*), 2) as pct_com_cupom
from pedidos;
