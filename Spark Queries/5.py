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


r_temp = region.filter("r_name = 'MIDDLE EAST'")
o_temp = orders.filter("o_orderdate >= '1994-01-01' ").filter("o_orderdate < '1995-01-01'")

c_o = customer.join(o_temp,customer.C_CUSTKEY == o_temp.O_CUSTKEY)
    .select('O_ORDERKEY')

r_n = r_temp.join(nation,r_temp.R_REGIONKEY == nation.N_REGIONKEY)
r_n_s = r_n.join(supplier,r_n.N_NATIONKEY == supplier.S_NATIONKEY)

r_n_s_l = r_n_s.join(lineitem,r_n_s.S_SUPPKEY == lineitem.L_SUPPKEY)
    .select('N_NAME','L_EXTENDEDPRICE','L_DISCOUNT','L_ORDERKEY')

r_n_s_l_c_o = r_n_s_l.join(c_o,r_n_s_l.L_ORDERKEY == c_o.O_ORDERKEY)
res = r_n_s_l_c_o.groupBy('N_NAME')
    .agg(F.sum(r_n_s_l_c_o.L_EXTENDEDPRICE *(1 -r_n_s_l_c_o.L_DISCOUNT)).alias("revenue"))
    .sort(F.desc("revenue"))

res.show()



