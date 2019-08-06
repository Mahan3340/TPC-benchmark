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


l_temp = lineitem.filter("l_shipdate >= '1994-01-01' ")
    .filter("l_shipdate < '1995-01-01' ").filter('l_discount >= 0.05')
    .filter('l_discount <= 0.07')
    .filter('l_quantity < 24')
res = l_temp.agg(F.sum(l_temp.L_EXTENDEDPRICE *(l_temp.L_DISCOUNT)).alias("revenue"))

res.show()
