BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1995-01-01'
	and o_orderdate < date '1995-01-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	
order by
	o_orderpriority
LIMIT 1;
COMMIT;


o_temp = orders.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1995-04-01'")
l_temp = lineitem.filter('l_commitdate < l_receiptdate')
l_o = l_temp.join(o_temp, l_temp.L_ORDERKEY == o_temp.O_ORDERKEY).groupBy('o_orderpriority').agg(F.count('*').alias("order_count")).select(o_temp.O_ORDERPRIORITY,'order_count').orderBy('o_orderpriority')