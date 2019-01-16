BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
LIMIT 100;
COMMIT;


lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)

datetime.datetime.now().time()
l_temp = lineitem.groupBy('L_ORDERKEY').agg(F.sum('L_QUANTITY').alias("SUM_QUANTITY")).filter('sum_quantity > 300').select(lineitem.L_ORDERKEY.alias("KEY"),'SUM_QUANTITY')
l_o = l_temp.join(orders,l_temp.KEY == orders.O_ORDERKEY)
l_o = l_o.join(lineitem,lineitem.L_ORDERKEY == l_o.KEY)
l_o_c = l_o.join(customer,customer.C_CUSTKEY == l_o.O_CUSTKEY).select('L_QUANTITY','C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE','O_TOTALPRICE').groupBy('C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE','O_TOTALPRICE').agg(F.sum('L_QUANTITY')).sort(F.desc("O_TOTALPRICE"),'O_ORDERDATE')
l_o_c.limit(100).show()
datetime.datetime.now().time()

