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

o_temp = orders.filter(" o_comment not like '%pending%deposits%' ")
c_o = customer.join(o_temp,customer.C_CUSTKEY == o_temp.O_CUSTKEY,"left_outer")
res1 = c_o.groupBy('O_ORDERKEY').agg(F.count('O_ORDERKEY').alias("C_COUNT"))
c_o = c_o.select('O_CUSTKEY',c_o.O_ORDERKEY.alias("O_ORDERKEY2"))

res2 = res1.join(c_o,c_o.O_ORDERKEY2 == res1.O_ORDERKEY)
    .select('C_COUNT','O_CUSTKEY','O_ORDERKEY')
    .groupBy('C_COUNT').agg(F.count('O_CUSTKEY').alias("CUSTDIST"))
    .sort(F.desc('CUSTDIST'),F.desc('C_COUNT'))

res2.show()









