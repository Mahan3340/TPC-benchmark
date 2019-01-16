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










