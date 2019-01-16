BEGIN; EXPLAIN (ANALYZE,TIMING OFF) 
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('20', '40', '22', '30', '39', '42', '21')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('20', '40', '22', '30', '39', '42', '21')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
LIMIT 1;
COMMIT;


val sub2 = udf { (x: String) => x.substring(0, 2) }
    val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
    val isNull = udf { (x: Any) => println(x); true }

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
      .filter(phone($"cntrycode"))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")

      //.filter("o_custkey is null")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")

datetime.datetime.now().time()
codes = ["20", "40", "22", "30", "39", "42", "21"]
c_temp = customer.select('C_ACCTBAL','C_CUSTKEY',customer.C_PHONE.substr(1,2).alias("COUNTRY_CODE"))
c_temp = c_temp.filter(c_temp.COUNTRY_CODE.isin(codes))
c_avg = c_temp.filter('c_acctbal > 0.0 ').agg(F.avg(c_temp.C_ACCTBAL).alias("AVG_ACCTBAL"))
o_c = orders.select('O_CUSTKEY').join(c_temp,orders.O_CUSTKEY == c_temp.C_CUSTKEY,"right_outer")
o_c_cavg = o_c.join(c_avg).filter(o_c.C_ACCTBAL > c_avg.AVG_ACCTBAL).groupBy(o_c.COUNTRY_CODE).agg(F.count(o_c.C_ACCTBAL),F.sum(o_c.C_ACCTBAL)).sort(o_c.COUNTRY_CODE)
o_c_cavg.show()
datetime.datetime.now().time()











