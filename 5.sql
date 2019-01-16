BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1994-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc
LIMIT 1;
COMMIT;



r_temp = region.filter("r_name = 'MIDDLE EAST'")
o_temp = orders.filter("o_orderdate >= '1994-01-01' ").filter("o_orderdate < '1995-01-01'")
c_o = customer.join(o_temp,customer.C_CUSTKEY == o_temp.O_CUSTKEY).select('O_ORDERKEY')
r_n = r_temp.join(nation,r_temp.R_REGIONKEY == nation.N_REGIONKEY)
r_n_s = r_n.join(supplier,r_n.N_NATIONKEY == supplier.S_NATIONKEY)
r_n_s_l = r_n_s.join(lineitem,r_n_s.S_SUPPKEY == lineitem.L_SUPPKEY).select('N_NAME','L_EXTENDEDPRICE','L_DISCOUNT','L_ORDERKEY')
r_n_s_l_c_o = r_n_s_l.join(c_o,r_n_s_l.L_ORDERKEY == c_o.O_ORDERKEY)
res = r_n_s_l_c_o.groupBy('N_NAME').agg(F.sum(r_n_s_l_c_o.L_EXTENDEDPRICE *(1 -r_n_s_l_c_o.L_DISCOUNT)).alias("revenue")).sort(F.desc("revenue"))



