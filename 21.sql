BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'EGYPT'
group by
	s_name
order by
	numwait desc,
	s_name
LIMIT 100;
COMMIT;



val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")
    //cache

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")
    // cache

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "SAUDI ARABIA")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")

      .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")

      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)


datetime.datetime.now().time()
s_temp = supplier.select('S_SUPPKEY','S_NATIONKEY','S_NAME')
l_temp = lineitem.select('L_SUPPKEY','L_ORDERKEY','L_RECEIPTDATE','L_COMMITDATE')
l_temp2 = l_temp.filter('l_receiptdate > l_commitdate')
o_temp = orders.select('O_ORDERKEY','O_ORDERSTATUS').filter("O_ORDERSTATUS = 'F' ")
n_temp = nation.filter("n_name like '%EGYPT%' ")

line1 = l_temp.groupBy(l_temp.L_ORDERKEY).agg( F.countDistinct(l_temp.L_SUPPKEY).alias("SUPPKEY_COUNT"),F.max(l_temp.L_SUPPKEY).alias("SUPPKEY_MAX") ).select(l_temp.L_ORDERKEY.alias("KEY"),"SUPPKEY_COUNT","SUPPKEY_MAX")

line2 = l_temp2.groupBy(l_temp.L_ORDERKEY).agg( F.countDistinct(l_temp.L_SUPPKEY).alias("SUPPKEY_COUNT"),F.max(l_temp.L_SUPPKEY).alias("SUPPKEY_MAX") ).select(l_temp.L_ORDERKEY.alias("KEY"),"SUPPKEY_COUNT","SUPPKEY_MAX")


n_s = n_temp.join(s_temp,s_temp.S_NATIONKEY == n_temp.N_NATIONKEY)
n_s_l = n_s.join(l_temp2,n_s.S_SUPPKEY == l_temp2.L_SUPPKEY)
n_s_l_o = n_s_l.join(o_temp,o_temp.O_ORDERKEY == n_s_l.L_ORDERKEY)
n_s_l_o_line1 = n_s_l_o.join(line1,line1.KEY == n_s_l_o.L_ORDERKEY).filter((line1.SUPPKEY_COUNT>1)|(line1.SUPPKEY_COUNT == 1)&(n_s_l_o.L_SUPPKEY == line1.SUPPKEY_MAX)).select(n_s_l_o.S_NAME,n_s_l_o.L_ORDERKEY,n_s_l_o.L_SUPPKEY)

n_s_l_o_line1_line2 = n_s_l_o_line1.join(line2,n_s_l_o_line1.L_ORDERKEY == line2.KEY,"left_outer").select(n_s_l_o_line1.S_NAME, n_s_l_o_line1.L_ORDERKEY, n_s_l_o_line1.L_SUPPKEY,line2.SUPPKEY_COUNT,line2. SUPPKEY_MAX).filter((line2.SUPPKEY_COUNT == 1)&(n_s_l_o_line1.L_SUPPKEY == line2.SUPPKEY_MAX)).groupBy(n_s_l_o_line1.S_NAME).agg(F.count(n_s_l_o_line1.L_SUPPKEY).alias("NUM_WAIT")).sort(F.desc("NUM_WAIT"),n_s_l_o_line1.S_NAME)
n_s_l_o_line1_line2.show()
datetime.datetime.now().time()









