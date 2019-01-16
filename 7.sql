BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'JAPAN' and n2.n_name = 'INDIA')
				or (n1.n_name = 'INDIA' and n2.n_name = 'JAPAN')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
LIMIT 1;
COMMIT;



n_temp = nation.filter("n_name  like 'JAPAN%' OR  n_name  like 'INDIA%' ")
l_temp = lineitem.filter("l_shipdate >= '1995-01-01' AND l_shipdate < '1996-12-31'")

s_n = n_temp.join(supplier,n_temp.N_NATIONKEY == supplier.S_NATIONKEY).join(l_temp,supplier.S_SUPPKEY == l_temp.L_SUPPKEY).withColumnRenamed("n_name", "supp_nation").select('supp_nation','l_orderkey','l_extendedprice','l_discount','l_shipdate')

res1 = n_temp.join(customer,n_temp.N_NATIONKEY == customer.C_NATIONKEY) .join(orders,customer.C_CUSTKEY == orders.O_CUSTKEY) .withColumnRenamed("n_name", "cust_nation") .select('cust_nation','o_orderkey') .join(s_n, orders.O_ORDERKEY == s_n.l_orderkey) .filter(("supp_nation like 'INDIA%'" and "cust_nation  like 'JAPAN%'") or ("supp_nation  like 'JAPAN%'" and "cust_nation  like 'INDIA%'"))
res2 = res1.select('supp_nation','cust_nation',F.year("l_shipdate").alias("l_year"),'l_discount','l_extendedprice').groupBy('supp_nation','cust_nation','l_year').agg(F.sum(res1.l_extendedprice * (1 - res1.l_discount)).alias("revenue")).orderBy('supp_nation','cust_nation','l_year')

res2.show()


































