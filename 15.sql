BEGIN; EXPLAIN (ANALYZE,TIMING OFF)
CREATE VIEW REVENUE0 (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from LINEITEM where l_shipdate >= date '1997-07-01' and l_shipdate < date '1997-07-01' + interval '3' month group by l_suppkey; select s_suppkey, s_name, s_address, s_phone, total_revenue from SUPPLIER, REVENUE0 where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from REVENUE0) order by s_suppkey; 
LIMIT 1; drop view REVENUE0;
COMMIT;

val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
      $"l_shipdate" < "1996-04-01")
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"))
    // .cache

    revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")

datetime.datetime.now().time()
l_temp = lineitem.filter("l_shipdate >= '1997-07-01'" and "l_shipdate < '1997-10-01'").select('L_SUPPKEY',(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).alias("VALUE"))
revenue = l_temp.groupBy("L_SUPPKEY").agg(F.sum("VALUE").alias("TOTAL"))
res1 = revenue.agg(F.max("TOTAL").alias("MAX_TOTAL")).collect()
max_total = res1[0].asDict()["MAX_TOTAL"]
res2 = revenue.filter(revenue.TOTAL == max_total).join(supplier,supplier.S_SUPPKEY == revenue.L_SUPPKEY).select('S_SUPPKEY','S_NAME','S_ADDRESS','S_PHONE','TOTAL').sort('S_SUPPKEY')
res2.show()
datetime.datetime.now().time()


