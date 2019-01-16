BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#31'
	and p_type not like 'LARGE PLATED%'
	and p_size in (48, 19, 12, 4, 41, 7, 21, 39)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size
LIMIT 1;
COMMIT;



 val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")

datetime.datetime.now().time()
sizes = [48, 19, 12, 4, 41, 7, 21, 39]
p_temp = part.filter("p_brand != 'Brand#31' " and "p_type not like 'LARGE PLATED%'" and part.P_SIZE.isin(sizes)).select('P_PARTKEY','P_BRAND','P_TYPE','P_SIZE')

s_ps = supplier.filter("s_comment like '%Customer%Complaints%' ").join(partsupp,supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
s_p_ps = s_ps.join(p_temp,s_ps.PS_PARTKEY == p_temp.P_PARTKEY).groupBy('P_BRAND','P_TYPE','P_SIZE').agg(F.count('PS_SUPPKEY').alias("SUPPLIER_COUNT")).sort(F.desc("SUPPLIER_COUNT"),'P_BRAND','P_TYPE','P_SIZE')
s_p_ps.show()
datetime.datetime.now().time()



