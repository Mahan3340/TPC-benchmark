BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('RAIL', 'FOB')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1997-01-01'
	and l_receiptdate < date '1997-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
LIMIT 1;
COMMIT;

 val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

lineitem.filter((
      $"l_shipmode" === "RAIL" || $"l_shipmode" === "FOB") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")

def isHigh(x):
 if "2-HIGH" in x or "1-URGENT" in x: return 1
 else: return 0

def isLow(x):
 if "2-HIGH" in x or "1-URGENT" in x: return 0
 else: return 1

isHigh_udf = F.udf(isHigh,IntegerType())
isLow_udf = F.udf(isLow,IntegerType())


l_temp = lineitem.filter(("l_shipmode = 'MAIL' " or "l_shipmode = 'SHIP' ") and 'l_commitdate < l_receiptdate' and 'l_shipdate < l_commitdate'  and "l_receiptdate >= '1997-01-01' " and " l_receiptdate <= '1998-01-01' ")
l_o = l_temp.join(orders,l_temp.L_ORDERKEY == orders.O_ORDERKEY).select('L_SHIPMODE' , 'O_ORDERPRIORITY')
res1 = l_o.groupBy('L_SHIPMODE').agg(F.sum(isHigh_udf(l_o.O_ORDERPRIORITY).alias("SUM_HIGHORDERPRIORITY")),F.sum(isLow_udf(l_o.O_ORDERPRIORITY).alias("SUM_LOWORDERPRIORITY"))).sort("L_SHIPMODE")

datetime.datetime.now().time()
res1.show()
datetime.datetime.now().time()














