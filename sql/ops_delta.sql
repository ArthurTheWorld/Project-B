create or replace temporary view pedidos using delta options (path '/data/delta/pedidos');
select _op, count(*) as qtd from pedidos group by _op order by qtd desc;
