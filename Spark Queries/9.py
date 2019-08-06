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

p_temp = part.filter("p_name like '%dim%'")
s_l = lineitem.join(supplier,lineitem.L_SUPPKEY == supplier.S_SUPPKEY)
s_l_ps = s_l.join((partsupp,s_l.L_SUPPKEY == partsupp.PS_SUPPKEY) & (s_l.L_PARTKEY == partsupp.PS_PARTKEY ))
s_l_ps_p = s_l_ps.join(part,s_l_ps.PS_PARTKEY == part.P_PARTKEY)
s_l_ps_p_o = s_l_ps_p.join(orders, s_l_ps_p.L_ORDERKEY == orders.O_ORDERKEY)
s_l_ps_p_o_n = s_l_ps_p_o.join(nation,s_l_ps_p_o.S_NATIONKEY == nation.N_NATIONKEY)
profit = s_l_ps_p_o_n
.select(s_l_ps_p_o_n.N_NAME.alias("NATION"),
        F.year(s_l_ps_p_o_n.O_ORDERDATE).alias("O_YEAR"),(s_l_ps_p_o_n.L_EXTENDEDPRICE * (1 - s_l_ps_p_o_n.L_DISCOUNT) - s_l_ps_p_o_n.PS_SUPPLYCOST * s_l_ps_p_o_n.L_QUANTITY).alias("AMOUNT"))

res = profit
    .select(profit.NATION,profit.AMOUNT,profit.O_YEAR)
    .groupBy(profit.NATION,profit.O_YEAR)
    .agg(F.sum(profit.AMOUNT).alias("SUM_PROFIT"))
    .orderBy(profit.NATION,F.desc("O_YEAR"))

p_temp = part.filter("p_name like '%dim%'")
l_p = p_temp.join(lineitem, p_temp.P_PARTKEY == lineitem.L_PARTKEY)
n_s = nation.join(supplier,nation.N_NATIONKEY == supplier.S_NATIONKEY)
l_p_s = l_p.join(n_s,l_p.L_SUPPKEY == n_s.S_SUPPKEY)
l_p_s_ps = l_p_s.join(partsupp,l_p_s.L_SUPPKEY == partsupp.PS_SUPPKEY)
l_p_s_ps_o = l_p_s_ps.join(orders,l_p_s_ps.L_ORDERKEY == orders.O_ORDERKEY)

profit = l_p_s_ps_o.select(l_p_s_ps_o.N_NAME,F.year(l_p_s_ps_o.O_ORDERDATE).alias("O_YEAR"),(l_p_s_ps_o.L_EXTENDEDPRICE * (1 - l_p_s_ps_o.L_DISCOUNT) - l_p_s_ps_o.PS_SUPPLYCOST * l_p_s_ps_o.L_QUANTITY).alias("AMOUNT"))

res = profit
    .groupBy(profit.N_NAME,profit.O_YEAR)
    .agg(F.sum(profit.AMOUNT).alias("SUM_PROFIT"))
    .orderBy(l_p_s_ps_o.N_NAME,F.desc("O_YEAR"))

res.show()











