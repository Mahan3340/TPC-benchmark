BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1996-12-01'
	and l_shipdate < date '1996-12-01' + interval '1' month
LIMIT 1;
COMMIT;



    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

datetime.datetime.now().time()
def promo(x,y):
 if x.startswith("PROMO"): return float(y)
 else: return float(0)

promo_udf = F.udf(promo,FloatType())

l_temp = lineitem.filter("l_shipdate>='1996-12-01'" and "l_shipdate<'1997-01-01'")
l_p = part.join(lineitem,part.P_PARTKEY == lineitem.L_PARTKEY)
res1 = l_p.select('P_TYPE',(l_p.L_EXTENDEDPRICE*(1-l_p.L_DISCOUNT)).alias("VALUE"))
res2 = res1.agg(F.sum("VALUE").alias("TOTAL_VALUE")).collect()
totalSum = res2[0].asDict()["TOTAL_VALUE"]
res3 = res1.agg(F.sum(promo_udf(res1.P_TYPE,res1.VALUE))*100/totalSum)
res3.show()
datetime.datetime.now().time()









