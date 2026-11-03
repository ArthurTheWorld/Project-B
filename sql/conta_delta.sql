create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');
select count(*) as total_delta from pedidos;