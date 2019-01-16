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


containers1 = ["SM CASE","SM BOX","SM PACK","SM PKG"]
containers2 = ["MED BAG","MED BOX","MED PKG","MED PACK"]
containers3 = ["LG CASE","LG BOX","LG PACK","LG PKG"]

p_l = part.
    join(lineitem,part.P_PARTKEY == lineitem.L_PARTKEY)
    .filter(("l_shipmode like '%AIR%' " or "l_shipmode like '%AIR REG%' ") and "l_shipinstruct like '%DELIVER IN PERSON%'")

p_l_2 = p_l
    .filter( ( (p_l.P_BRAND.like("%Brand#54%")) & (p_l.P_CONTAINER.isin(containers1)) & (p_l.L_QUANTITY >= 8) & (p_l.L_QUANTITY <= 18) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 10) ) | ((p_l.P_BRAND.like("%Brand#22%")) & (p_l.P_CONTAINER.isin(containers2)) & (p_l.L_QUANTITY >= 3) & (p_l.L_QUANTITY <= 23) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 5) ) | ((p_l.P_BRAND.like("%Brand#51%")) & (p_l.P_CONTAINER.isin(containers3)) & (p_l.L_QUANTITY >= 22) & (p_l.L_QUANTITY <= 32) &  (p_l.P_SIZE >= 1) & (p_l.P_SIZE <= 15) ) ).select((p_l.L_EXTENDEDPRICE*(1-p_l.L_DISCOUNT)).alias("VOLUME")).agg(F.sum("VOLUME"))

p_l_2.show()














