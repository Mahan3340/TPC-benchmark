BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part,
	(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM lineitem GROUP BY l_partkey) part_agg
where
	p_partkey = l_partkey
	and agg_partkey = l_partkey
	and p_brand = 'Brand#44'
	and p_container = 'WRAP PKG'
	and l_quantity < avg_quantity
LIMIT 1;
COMMIT;



val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)



datetime.datetime.now().time()
l_temp = lineitem.select('L_PARTKEY','L_QUANTITY','L_EXTENDEDPRICE')
p_temp = part.filter("p_brand like '%Brand#44%' " and "p_container like '%WRAP PKG%' ")
p_l = p_temp.join(l_temp,p_temp.P_PARTKEY == l_temp.L_PARTKEY,"left_outer")
p_temp2 = p_l.groupBy("P_PARTKEY").agg(F.avg(p_l.L_QUANTITY*0.2).alias("AVG_QUANTITY")).select(p_l.P_PARTKEY.alias("KEY"),'AVG_QUANTITY')
p_temp2 = p_temp2.join(p_l,p_temp2.KEY == p_l.P_PARTKEY).select('AVG_QUANTITY','KEY','L_QUANTITY','L_EXTENDEDPRICE')
p_p = p_temp.join(p_temp2,p_temp2.KEY == p_temp.P_PARTKEY).filter(p_temp2.L_QUANTITY < p_temp2.AVG_QUANTITY).agg((F.sum(p_temp2.L_EXTENDEDPRICE)/7.0).alias("AVG_YEARLY"))
p_p.show()
datetime.datetime.now().time()


