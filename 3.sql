BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-13'
	and l_shipdate > date '1995-03-13'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;
COMMIT;

c_temp = customer.filter("c_mktsegment = 'AUTOMOBILE'")
l_temp = lineitem.filter("l_shipdate > '1995-03-13' ")
o_temp = orders.filter("o_orderdate < '1995-03-13' ")
l_o = l_temp.join(o_temp,l_temp.L_ORDERKEY == o_temp.O_ORDERKEY)
c_o = c_temp.join(l_o,c_temp.C_CUSTKEY == l_o.O_CUSTKEY)

res = c_o.select(c_o.O_ORDERKEY,c_o.O_ORDERDATE,c_o.O_SHIPPRIORITY).join(l_temp,c_o.O_ORDERKEY == l_temp.L_ORDERKEY).select(c_o.O_ORDERKEY,c_o.O_ORDERDATE,c_o.O_SHIPPRIORITY,l_temp.L_ORDERKEY,c_o.O_ORDERKEY,c_o.O_ORDERDATE,l_temp.L_EXTENDEDPRICE,l_temp.L_DISCOUNT)
res2 = res.groupBy('l_orderkey', 'o_orderdate', 'o_shippriority').agg(F.sum(res.L_EXTENDEDPRICE *(1 -l_temp.L_DISCOUNT)).alias("revenue")).sort(res.O_ORDERDATE,F.desc("revenue")) 





