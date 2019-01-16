
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

s_temp = supplier.select('S_SUPPKEY','S_NATIONKEY','S_NAME')
l_temp = lineitem.select('L_SUPPKEY','L_ORDERKEY','L_RECEIPTDATE','L_COMMITDATE')
l_temp2 = l_temp.filter('l_receiptdate > l_commitdate')
o_temp = orders.select('O_ORDERKEY','O_ORDERSTATUS').filter("O_ORDERSTATUS = 'F' ")
n_temp = nation.filter("n_name like '%EGYPT%' ")

line1 = l_temp
    .groupBy(l_temp.L_ORDERKEY)
    .agg(
       F.countDistinct(l_temp.L_SUPPKEY).alias("SUPPKEY_COUNT"),
       F.max(l_temp.L_SUPPKEY).alias("SUPPKEY_MAX") )
    .select(l_temp.L_ORDERKEY.alias("KEY"),"SUPPKEY_COUNT","SUPPKEY_MAX")

line2 = l_temp2
    .groupBy(l_temp.L_ORDERKEY)
    .agg(
         F.countDistinct(l_temp.L_SUPPKEY).alias("SUPPKEY_COUNT"),
         F.max(l_temp.L_SUPPKEY).alias("SUPPKEY_MAX") )
    .select(l_temp.L_ORDERKEY.alias("KEY"),"SUPPKEY_COUNT","SUPPKEY_MAX")


n_s = n_temp.join(s_temp,s_temp.S_NATIONKEY == n_temp.N_NATIONKEY)
n_s_l = n_s.join(l_temp2,n_s.S_SUPPKEY == l_temp2.L_SUPPKEY)
n_s_l_o = n_s_l.join(o_temp,o_temp.O_ORDERKEY == n_s_l.L_ORDERKEY)
n_s_l_o_line1 = n_s_l_o
    .join(line1,line1.KEY == n_s_l_o.L_ORDERKEY)
    .filter((line1.SUPPKEY_COUNT>1)|(line1.SUPPKEY_COUNT == 1)&(n_s_l_o.L_SUPPKEY == line1.SUPPKEY_MAX))
    .select(n_s_l_o.S_NAME,n_s_l_o.L_ORDERKEY,n_s_l_o.L_SUPPKEY)

n_s_l_o_line1_line2 = n_s_l_o_line1.join(line2,n_s_l_o_line1.L_ORDERKEY == line2.KEY,"left_outer")
    .select(n_s_l_o_line1.S_NAME, n_s_l_o_line1.L_ORDERKEY, n_s_l_o_line1.L_SUPPKEY,line2.SUPPKEY_COUNT,line2. SUPPKEY_MAX).filter((line2.SUPPKEY_COUNT == 1)&(n_s_l_o_line1.L_SUPPKEY == line2.SUPPKEY_MAX))
    .groupBy(n_s_l_o_line1.S_NAME)
    .agg(F.count(n_s_l_o_line1.L_SUPPKEY).alias("NUM_WAIT"))
    .sort(F.desc("NUM_WAIT"),n_s_l_o_line1.S_NAME)

n_s_l_o_line1_line2.show()










