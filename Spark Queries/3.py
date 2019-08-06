
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


c_temp = customer.filter("c_mktsegment = 'AUTOMOBILE'")
l_temp = lineitem.filter("l_shipdate > '1995-03-13' ")
o_temp = orders.filter("o_orderdate < '1995-03-13' ")
l_o = l_temp.join(o_temp,l_temp.L_ORDERKEY == o_temp.O_ORDERKEY)
c_o = c_temp.join(l_o,c_temp.C_CUSTKEY == l_o.O_CUSTKEY)

res = c_o
    .select(c_o.O_ORDERKEY,c_o.O_ORDERDATE,c_o.O_SHIPPRIORITY)
    .join(l_temp,c_o.O_ORDERKEY == l_temp.L_ORDERKEY)
    .select(c_o.O_ORDERKEY,c_o.O_ORDERDATE,c_o.O_SHIPPRIORITY,l_temp.L_ORDERKEY,c_o.O_ORDERKEY,c_o.O_ORDERDATE,l_temp.L_EXTENDEDPRICE,l_temp.L_DISCOUNT)
res2 = res
    .groupBy('l_orderkey', 'o_orderdate', 'o_shippriority')
    .agg(F.sum(res.L_EXTENDEDPRICE *(1 -l_temp.L_DISCOUNT)).alias("revenue"))
    .sort(res.O_ORDERDATE,F.desc("revenue"))

res2.show()





