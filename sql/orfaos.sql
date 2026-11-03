create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');
select count(*) as total_delta from pedidos;
select count(*) as orfaos from pedidos p where p.pedido_id not in (select pedido_id from origem);
