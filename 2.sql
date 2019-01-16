BEGIN; EXPLAIN (ANALYZE,TIMING OFF) select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 30
	and p_type like '%STEEL'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region,
		        part
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'ASIA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
LIMIT 100;
COMMIT;



r_temp = region.filter("r_name = 'ASIA' ")
n_r = nation.join(r_temp,nation.N_REGIONKEY == r_temp.R_REGIONKEY)
p_temp = part.filter('p_size = 30').filter("p_type like '%STEEL'")
n_r_s1 = n_r.join(supplier,supplier.S_NATIONKEY == N_NATIONKEY)
n_r_s_ps1 = n_r_s1.join(partsupp,partsupp.PS_SUPPKEY == S_SUPPKEY)
n_r_s_ps_p1 = n_r_s_ps1.join(part,part.P_PARTKEY == partsupp.PS_PARTKEY)
min_supp_cost = n_r_s_ps_p1.agg(F.min('ps_supplycost')).collect()
min = min_supp_cost[0].asDict()["min(ps_supplycost)"]
ps_temp = partsupp.filter(partsupp.PS_SUPPLYCOST == min)
n_r_s = n_r.join(supplier,n_r.N_NATIONKEY == supplier.S_NATIONKEY)
n_r_s_ps = n_r_s.join(ps_temp,ps_temp.PS_SUPPKEY == n_r_s.S_SUPPKEY)
al = n_r_s_ps.join(p_temp,p_temp.P_PARTKEY == n_r_s_ps.PS_PARTKEY).select('s_acctbal','s_name','n_name','p_partkey','p_mfgr','s_address','s_phone','s_comment').orderBy(F.desc("s_acctbal"),'n_name','s_name','p_partkey').limit(100)



