BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-08-01'
	and o_orderdate < date '1993-08-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;
COMMIT;


l_temp = lineitem.filter("l_returnflag == 'R' ")
o_temp = orders.filter("o_orderdate >= '1994-01-01' ").filter("o_orderdate < '1994-4-01'")
o_c = o_temp.join(customer,o_temp.O_CUSTKEY == customer.C_CUSTKEY)
o_c_n = o_c.join(nation,o_c.C_NATIONKEY == nation.N_NATIONKEY)
o_c_n_l = o_c_n.join(l_temp,l_temp.O_ORDERKEY == o_c_n.O_ORDERKEY)
res = o_c_n_l.select('C_CUSTKEY','C_NAME',(o_c_n_l.L_EXTENDEDPRICE*(1-o_c_n_l.L_DISCOUNT)).alias("VOLUME"),'C_ACCTBAL','N_NAME','C_ADDRESS','C_PHONE','C_COMMENT','C_PHONE')
res2 = res.groupBy('C_CUSTKEY','C_NAME','C_ACCTBAL','N_NAME','C_ADDRESS','C_COMMENT','C_PHONE').agg(F.sum("VOLUME").alias("REVENUE"))


datetime.datetime.now().time()
res2.limit(10).show()
datetime.datetime.now().time()








