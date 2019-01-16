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

isHigh_udf = F.udf(isHigh,IntegerType())
isLow_udf = F.udf(isLow,IntegerType())


l_temp = lineitem
    .filter(("l_shipmode = 'MAIL' " or "l_shipmode = 'SHIP' ") and 'l_commitdate < l_receiptdate' and 'l_shipdate < l_commitdate'  and "l_receiptdate >= '1997-01-01' " and " l_receiptdate <= '1998-01-01' ")
l_o = l_temp.
    join(orders,l_temp.L_ORDERKEY == orders.O_ORDERKEY)
    .select('L_SHIPMODE' , 'O_ORDERPRIORITY')

res1 = l_o.groupBy('L_SHIPMODE')
    .agg(
         F.sum(isHigh_udf(l_o.O_ORDERPRIORITY).alias("SUM_HIGHORDERPRIORITY")),
         F.sum(isLow_udf(l_o.O_ORDERPRIORITY).alias("SUM_LOWORDERPRIORITY")))
    .sort("L_SHIPMODE")

res1.show()














