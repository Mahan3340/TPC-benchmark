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


n_temp = nation.filter("n_name like '%ARGENTINA%' ")
n_s = n_temp
    .join(supplier,nation.N_NATIONKEY == supplier.S_NATIONKEY)
    .select('S_SUPPKEY')

n_s_ps = n_s
    .join(partsupp,n_s.S_SUPPKEY == partsupp.PS_SUPPKEY)
    .select('PS_PARTKEY',(partsupp.PS_SUPPLYCOST*partsupp.PS_AVAILQTY).alias("VALUE"))

sum = n_s_ps.agg(F.sum("VALUE").alias("TOTAL_VALUE"))
res1 = n_s_ps.groupBy('PS_PARTKEY').agg(F.sum("VALUE").alias("PART_VALUE"))

res2 = res1.
crossJoin(sum)
    .filter(res1.PART_VALUE > (sum.TOTAL_VALUE*0.0001))
    .sort(F.desc("PART_VALUE"))

res2.show()






