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

sizes = [48, 19, 12, 4, 41, 7, 21, 39]
p_temp = part
    .filter("p_brand != 'Brand#31' " and "p_type not like 'LARGE PLATED%'" and part.P_SIZE.isin(sizes))
    .select('P_PARTKEY','P_BRAND','P_TYPE','P_SIZE')

s_ps = supplier
    .filter("s_comment like '%Customer%Complaints%' ")
    .join(partsupp,supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
s_p_ps = s_ps
    .join(p_temp,s_ps.PS_PARTKEY == p_temp.P_PARTKEY)
    .groupBy('P_BRAND','P_TYPE','P_SIZE')
    .agg(F.count('PS_SUPPKEY').alias("SUPPLIER_COUNT"))
    .sort(F.desc("SUPPLIER_COUNT"),'P_BRAND','P_TYPE','P_SIZE')

s_p_ps.show()



