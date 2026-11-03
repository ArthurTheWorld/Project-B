create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');
select min(pedido_id) as min_id, max(pedido_id) as max_id, count(*) as total from pedidos;
