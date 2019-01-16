BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'ARGENTINA'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'ARGENTINA'
		)
order by
	value desc
LIMIT 1;
COMMIT;



val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)


n_temp = nation.filter("n_name like '%ARGENTINA%' ")
n_s = n_temp.join(supplier,nation.N_NATIONKEY == supplier.S_NATIONKEY).select('S_SUPPKEY')
n_s_ps = n_s.join(partsupp,n_s.S_SUPPKEY == partsupp.PS_SUPPKEY).select('PS_PARTKEY',(partsupp.PS_SUPPLYCOST*partsupp.PS_AVAILQTY).alias("VALUE"))
sum = n_s_ps.agg(F.sum("VALUE").alias("TOTAL_VALUE"))
res1 = n_s_ps.groupBy('PS_PARTKEY').agg(F.sum("VALUE").alias("PART_VALUE"))
res2 = res1.crossJoin(sum).filter(res1.PART_VALUE > (sum.TOTAL_VALUE*0.0001)).sort(F.desc("PART_VALUE"))

datetime.datetime.now().time()
res2.show()
datetime.datetime.now().time()






