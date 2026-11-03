create or replace temporary view p using delta options (path '/data/delta/pedidos');
select 'total' as m, cast(count(*) as string) as v from p
union all
select 'min_id', cast(min(pedido_id) as string) from p
union all
select 'max_id', cast(max(pedido_id) as string) from p
union all
select 'op_c', cast(sum(case when _op='c' then 1 else 0 end) as string) from p
union all
select 'op_u', cast(sum(case when _op='u' then 1 else 0 end) as string) from p
union all
select 'op_d', cast(sum(case when _op='d' then 1 else 0 end) as string) from p;
