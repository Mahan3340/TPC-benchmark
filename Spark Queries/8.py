
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


def isJAPAN(x,y):
    if "JAPAN" in x: return float(y)
    else: return float(0)

isJAPAN_udf = F.udf(isJAPAN,FloatType())

r_temp = region.filter("r_name = 'AMERICA' ") 
o_temp = orders.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1996-12-31'")
p_temp = part.filter("p_type == 'ECONOMY ANODIZED STEEL'")
n_s = nation.join(supplier,supplier.S_NATIONKEY == nation.N_NATIONKEY)
l_p = lineitem.join(p_temp,lineitem.L_PARTKEY == p_temp.P_PARTKEY)

l_p_n_s = l_p.join(n_s,l_p.L_SUPPKEY == n_s.S_SUPPKEY)
.select('L_PARTKEY','L_SUPPKEY','L_ORDERKEY',(l_p.L_EXTENDEDPRICE*(1-l_p.L_DISCOUNT)).alias("VOLUME"))

n_r = nation.join(r_temp,nation.N_REGIONKEY == r_temp.R_REGIONKEY).select('N_NATIONKEY','N_NAME')
n_r_c = n_r.join(customer,n_r.N_NATIONKEY == customer.C_NATIONKEY).select('C_CUSTKEY','N_NAME')
n_r_c_o = n_r_c.join(o_temp,n_r_c.C_CUSTKEY == o_temp.O_CUSTKEY).select('O_ORDERKEY','O_ORDERDATE','N_NAME')

n_r_c_o_l = n_r_c_o.join(l_p_n_s,n_r_c_o.O_ORDERKEY == l_p_n_s.L_ORDERKEY)
res1 = n_r_c_o_l.select(F.year("O_ORDERDATE").alias("o_year"),'VOLUME',isJAPAN_udf(n_r_c_o_l.N_NAME,n_r_c_o_l.VOLUME).alias("JAPAN_VOLUME"))
res2 = res1.groupBy('o_year')
    .agg(F.sum(res1.VOLUME).alias("TOTAL_VOLUME"))
    .orderBy('o_year')
    .select(res1.o_year.alias("o_year2"),'TOTAL_VOLUME')

res3 = res1
    .groupBy('o_year')
    .agg(F.sum(res1.JAPAN_VOLUME).alias("TOTAL_JAPAN"))
    .orderBy('o_year')
    .select(res1.o_year.alias("o_year3"),'TOTAL_JAPAN')

res4 = res3
    .join(res2,res2.o_year2 == res3.o_year3)
    .dropDuplicates()
    .select(res3.o_year3.alias("o_year"),(res3.TOTAL_JAPAN/res2.TOTAL_VOLUME).alias("mkt_share"))

res4.show()


















