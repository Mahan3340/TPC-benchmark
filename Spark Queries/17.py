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


l_temp = lineitem.select('L_PARTKEY','L_QUANTITY','L_EXTENDEDPRICE')
p_temp = part
    .filter("p_brand like '%Brand#44%' " and "p_container like '%WRAP PKG%' ")

p_l = p_temp.join(l_temp,p_temp.P_PARTKEY == l_temp.L_PARTKEY,"left_outer")
p_temp2 = p_l
    .groupBy("P_PARTKEY")
    .agg(F.avg(p_l.L_QUANTITY*0.2).alias("AVG_QUANTITY"))
    .select(p_l.P_PARTKEY.alias("KEY"),'AVG_QUANTITY')

p_temp2 = p_temp2
    .join(p_l,p_temp2.KEY == p_l.P_PARTKEY)
    .select('AVG_QUANTITY','KEY','L_QUANTITY','L_EXTENDEDPRICE')

p_p = p_temp
    .join(p_temp2,p_temp2.KEY == p_temp.P_PARTKEY)
    .filter(p_temp2.L_QUANTITY < p_temp2.AVG_QUANTITY)
    .agg((F.sum(p_temp2.L_EXTENDEDPRICE)/7.0).alias("AVG_YEARLY"))

p_p.show()



