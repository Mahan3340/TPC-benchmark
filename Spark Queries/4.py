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

o_temp = orders.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1995-04-01'")
l_temp = lineitem.filter('l_commitdate < l_receiptdate')
l_o = l_temp.join(o_temp, l_temp.L_ORDERKEY == o_temp.O_ORDERKEY)
    .groupBy('o_orderpriority')
    .agg(F.count('*').alias("order_count"))
    .select(o_temp.O_ORDERPRIORITY,'order_count')
    .orderBy('o_orderpriority')

l_o.show()
