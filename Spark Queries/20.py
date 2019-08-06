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


joinCondition = [l_temp.L_SUPPKEY == p_ps.PS_SUPPKEY, l_temp.L_PARTKEY == p_ps.PS_PARTKEY]

l_temp = lineitem
    .filter("l_shipdate >= '1993-01-01' " and "l_shipdate < '1994-01-01' ")
    .groupBy('L_PARTKEY','L_SUPPKEY')
    .agg(F.sum(lineitem.L_QUANTITY*0.5).alias("SUM_QUANTITY"))

n_temp = nation.filter("n_name like '%CANADA%' ")
n_s = supplier
    .select('S_SUPPKEY','S_NAME','S_NATIONKEY','S_ADDRESS')
    .join(n_temp,supplier.S_NATIONKEY == n_temp.N_NATIONKEY)

p_temp = part
    .filter(part.P_NAME.startswith("forest"))
    .select('P_PARTKEY')

p_ps = p_temp
    .join(partsupp,part.P_PARTKEY == partsupp.PS_PARTKEY)

p_ps_l = p_ps
    .join(l_temp,joinCondition)
    .filter(p_ps.PS_AVAILQTY > l_temp.SUM_QUANTITY).select(p_ps.PS_SUPPKEY)
p_ps_l_n_s = p_ps_l
    .join(n_s,n_s.S_SUPPKEY == p_ps_l.PS_SUPPKEY)
    .select(n_s.S_NAME,n_s.S_ADDRESS)
    .sort(n_s.S_NAME)

p_ps_l_n_s.show()




