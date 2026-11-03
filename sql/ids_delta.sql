-- Exporta os pedido_id vivos no Delta, um por linha, para comparacao externa.
create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');
select pedido_id from pedidos order by pedido_id;
