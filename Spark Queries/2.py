
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

r_temp = region.filter("r_name = 'ASIA' ")
n_r = nation.join(r_temp,nation.N_REGIONKEY == r_temp.R_REGIONKEY)
p_temp = part.filter('p_size = 30').filter("p_type like '%STEEL'")
n_r_s1 = n_r.join(supplier,supplier.S_NATIONKEY == N_NATIONKEY)
n_r_s_ps1 = n_r_s1.join(partsupp,partsupp.PS_SUPPKEY == S_SUPPKEY)
n_r_s_ps_p1 = n_r_s_ps1.join(part,part.P_PARTKEY == partsupp.PS_PARTKEY)
min_supp_cost = n_r_s_ps_p1.agg(F.min('ps_supplycost')).collect()
min = min_supp_cost[0].asDict()["min(ps_supplycost)"]
ps_temp = partsupp.filter(partsupp.PS_SUPPLYCOST == min)
n_r_s = n_r.join(supplier,n_r.N_NATIONKEY == supplier.S_NATIONKEY)
n_r_s_ps = n_r_s.join(ps_temp,ps_temp.PS_SUPPKEY == n_r_s.S_SUPPKEY)
al = n_r_s_ps.join(p_temp,p_temp.P_PARTKEY == n_r_s_ps.PS_PARTKEY)
    .select('s_acctbal','s_name','n_name','p_partkey','p_mfgr','s_address','s_phone','s_comment')
    .orderBy(F.desc("s_acctbal"),'n_name','s_name','p_partkey')
    .limit(100)

n_r_s_ps.show()



