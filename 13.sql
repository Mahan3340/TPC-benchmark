BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%pending%deposits%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
LIMIT 1;
COMMIT;

val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)



o_temp = orders.filter(" o_comment not like '%pending%deposits%' ")
c_o = customer.join(o_temp,customer.C_CUSTKEY == o_temp.O_CUSTKEY,"left_outer")
res1 = c_o.groupBy('O_ORDERKEY').agg(F.count('O_ORDERKEY').alias("C_COUNT"))
c_o = c_o.select('O_CUSTKEY',c_o.O_ORDERKEY.alias("O_ORDERKEY2"))
res2 = res1.join(c_o,c_o.O_ORDERKEY2 == res1.O_ORDERKEY).select('C_COUNT','O_CUSTKEY','O_ORDERKEY').groupBy('C_COUNT').agg(F.count('O_CUSTKEY').alias("CUSTDIST")).sort(F.desc('CUSTDIST'),F.desc('C_COUNT'))


datetime.datetime.now().time()
res2.show()
datetime.datetime.now().time()









