from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F


orders = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/orders.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/lineitem.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/nation.parquet")
region = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/region.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/part.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/supplier.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/partsupp.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/customer.parquet")

n_temp = nation.filter("n_name  like 'JAPAN%' OR  n_name  like 'INDIA%' ")
l_temp = lineitem.filter("l_shipdate >= '1995-01-01' AND l_shipdate < '1996-12-31'")

s_n = n_temp
.join(supplier,n_temp.N_NATIONKEY == supplier.S_NATIONKEY).join(l_temp,supplier.S_SUPPKEY == l_temp.L_SUPPKEY).withColumnRenamed("n_name", "supp_nation")
.select('supp_nation','l_orderkey','l_extendedprice','l_discount','l_shipdate')

res1 = n_temp
    .join(customer,n_temp.N_NATIONKEY == customer.C_NATIONKEY)
    .join(orders,customer.C_CUSTKEY == orders.O_CUSTKEY)
    .withColumnRenamed("n_name", "cust_nation")
    .select('cust_nation','o_orderkey')
    .join(s_n, orders.O_ORDERKEY == s_n.l_orderkey)
    .filter(("supp_nation like 'INDIA%'" and "cust_nation  like 'JAPAN%'") or ("supp_nation  like 'JAPAN%'" and "cust_nation  like 'INDIA%'"))

res2 = res1
    .select('supp_nation','cust_nation',F.year("l_shipdate").alias("l_year"),'l_discount','l_extendedprice')
    .groupBy('supp_nation','cust_nation','l_year').agg(F.sum(res1.l_extendedprice * (1 - res1.l_discount)).alias("revenue"))
    .orderBy('supp_nation','cust_nation','l_year')

res2.show()


































