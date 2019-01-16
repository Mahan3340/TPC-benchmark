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

l_temp = lineitem.filter("l_returnflag == 'R' ")

o_temp = orders
    .filter("o_orderdate >= '1994-01-01' ")
    .filter("o_orderdate < '1994-4-01'")

o_c = o_temp.join(customer,o_temp.O_CUSTKEY == customer.C_CUSTKEY)
o_c_n = o_c.join(nation,o_c.C_NATIONKEY == nation.N_NATIONKEY)
o_c_n_l = o_c_n.join(l_temp,l_temp.O_ORDERKEY == o_c_n.O_ORDERKEY)

res = o_c_n_l
    .select('C_CUSTKEY','C_NAME',(o_c_n_l.L_EXTENDEDPRICE*(1-o_c_n_l.L_DISCOUNT)).alias("VOLUME"),'C_ACCTBAL','N_NAME','C_ADDRESS','C_PHONE','C_COMMENT','C_PHONE')

res2 = res
    .groupBy('C_CUSTKEY','C_NAME','C_ACCTBAL','N_NAME','C_ADDRESS','C_COMMENT','C_PHONE')
    .agg(F.sum("VOLUME").alias("REVENUE"))

res2.limit(10).show()









