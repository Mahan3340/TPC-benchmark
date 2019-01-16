BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%dim%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
LIMIT 1;
COMMIT;




p_temp = part.filter("p_name like '%dim%'")
s_l = lineitem.join(supplier,lineitem.L_SUPPKEY == supplier.S_SUPPKEY)
s_l_ps = s_l.join((partsupp,s_l.L_SUPPKEY == partsupp.PS_SUPPKEY) & (s_l.L_PARTKEY == partsupp.PS_PARTKEY ))
s_l_ps_p = s_l_ps.join(part,s_l_ps.PS_PARTKEY == part.P_PARTKEY)
s_l_ps_p_o = s_l_ps_p.join(orders, s_l_ps_p.L_ORDERKEY == orders.O_ORDERKEY)
s_l_ps_p_o_n = s_l_ps_p_o.join(nation,s_l_ps_p_o.S_NATIONKEY == nation.N_NATIONKEY)
profit = s_l_ps_p_o_n.select(s_l_ps_p_o_n.N_NAME.alias("NATION"),F.year(s_l_ps_p_o_n.O_ORDERDATE).alias("O_YEAR"),(s_l_ps_p_o_n.L_EXTENDEDPRICE * (1 - s_l_ps_p_o_n.L_DISCOUNT) - s_l_ps_p_o_n.PS_SUPPLYCOST * s_l_ps_p_o_n.L_QUANTITY).alias("AMOUNT"))
res = profit.select(profit.NATION,profit.AMOUNT,profit.O_YEAR).groupBy(profit.NATION,profit.O_YEAR).agg(F.sum(profit.AMOUNT).alias("SUM_PROFIT")).orderBy(profit.NATION,F.desc("O_YEAR"))





val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)


p_temp = part.filter("p_name like '%dim%'")
l_p = p_temp.join(lineitem, p_temp.P_PARTKEY == lineitem.L_PARTKEY)
n_s = nation.join(supplier,nation.N_NATIONKEY == supplier.S_NATIONKEY)
l_p_s = l_p.join(n_s,l_p.L_SUPPKEY == n_s.S_SUPPKEY)
l_p_s_ps = l_p_s.join(partsupp,l_p_s.L_SUPPKEY == partsupp.PS_SUPPKEY)
l_p_s_ps_o = l_p_s_ps.join(orders,l_p_s_ps.L_ORDERKEY == orders.O_ORDERKEY)
profit = l_p_s_ps_o.select(l_p_s_ps_o.N_NAME,F.year(l_p_s_ps_o.O_ORDERDATE).alias("O_YEAR"),(l_p_s_ps_o.L_EXTENDEDPRICE * (1 - l_p_s_ps_o.L_DISCOUNT) - l_p_s_ps_o.PS_SUPPLYCOST * l_p_s_ps_o.L_QUANTITY).alias("AMOUNT"))
res = profit.groupBy(profit.N_NAME,profit.O_YEAR).agg(F.sum(profit.AMOUNT).alias("SUM_PROFIT")).orderBy(l_p_s_ps_o.N_NAME,F.desc("O_YEAR"))

datetime.datetime.now().time()
res.show()
datetime.datetime.now().time()













