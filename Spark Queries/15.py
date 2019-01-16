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

l_temp = lineitem
    .filter("l_shipdate >= '1997-07-01'" and "l_shipdate < '1997-10-01'")
    .select('L_SUPPKEY',(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).alias("VALUE"))

revenue = l_temp
    .groupBy("L_SUPPKEY")
    .agg(F.sum("VALUE").alias("TOTAL"))

res1 = revenue.agg(F.max("TOTAL").alias("MAX_TOTAL")).collect()
max_total = res1[0].asDict()["MAX_TOTAL"]

res2 = revenue
    .filter(revenue.TOTAL == max_total)
    .join(supplier,supplier.S_SUPPKEY == revenue.L_SUPPKEY)
    .select('S_SUPPKEY','S_NAME','S_ADDRESS','S_PHONE','TOTAL')
    .sort('S_SUPPKEY')

res2.show()



