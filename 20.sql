BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp,
			(
				select
					l_partkey agg_partkey,
					l_suppkey agg_suppkey,
					0.5 * sum(l_quantity) AS agg_quantity
				from
					lineitem
				where
					l_shipdate >= date '1993-01-01'
					and l_shipdate < date '1993-01-01' + interval '1' year
				group by
					l_partkey,
					l_suppkey
			) agg_lineitem
		where
			agg_partkey = ps_partkey
			and agg_suppkey = ps_suppkey
			and ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'green%'
			)
			and ps_availqty > agg_quantity
	)
	and s_nationkey = n_nationkey
	and n_name = 'ALGERIA'
order by
	s_name
LIMIT 1;
COMMIT;


datetime.datetime.now().time()
joinCondition = [l_temp.L_SUPPKEY == p_ps.PS_SUPPKEY, l_temp.L_PARTKEY == p_ps.PS_PARTKEY]
l_temp = lineitem.filter("l_shipdate >= '1993-01-01' " and "l_shipdate < '1994-01-01' ").groupBy('L_PARTKEY','L_SUPPKEY').agg(F.sum(lineitem.L_QUANTITY*0.5).alias("SUM_QUANTITY"))
n_temp = nation.filter("n_name like '%CANADA%' ")
n_s = supplier.select('S_SUPPKEY','S_NAME','S_NATIONKEY','S_ADDRESS').join(n_temp,supplier.S_NATIONKEY == n_temp.N_NATIONKEY)
p_temp = part.filter(part.P_NAME.startswith("forest")).select('P_PARTKEY')
p_ps = p_temp.join(partsupp,part.P_PARTKEY == partsupp.PS_PARTKEY)
p_ps_l = p_ps.join(l_temp,joinCondition).filter(p_ps.PS_AVAILQTY > l_temp.SUM_QUANTITY).select(p_ps.PS_SUPPKEY)
p_ps_l_n_s = p_ps_l.join(n_s,n_s.S_SUPPKEY == p_ps_l.PS_SUPPKEY).select(n_s.S_NAME,n_s.S_ADDRESS).sort(n_s.S_NAME)
datetime.datetime.now().time()
p_ps_l_n_s.show()
datetime.datetime.now().time()



