create or replace temporary view p using delta options (path '/data/delta/pedidos');
select _op, count(*) as c from p group by _op;
