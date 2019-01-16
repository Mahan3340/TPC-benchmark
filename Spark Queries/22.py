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

codes = ["20", "40", "22", "30", "39", "42", "21"]
c_temp = customer.select('C_ACCTBAL','C_CUSTKEY',customer.C_PHONE.substr(1,2).alias("COUNTRY_CODE"))
c_temp = c_temp.filter(c_temp.COUNTRY_CODE.isin(codes))

c_avg = c_temp
    .filter('c_acctbal > 0.0 ')
    .agg(F.avg(c_temp.C_ACCTBAL).alias("AVG_ACCTBAL"))

o_c = orders
    .select('O_CUSTKEY')
    .join(c_temp,orders.O_CUSTKEY == c_temp.C_CUSTKEY,"right_outer")

o_c_cavg = o_c
    .join(c_avg).filter(o_c.C_ACCTBAL > c_avg.AVG_ACCTBAL)
    .groupBy(o_c.COUNTRY_CODE)
    .agg(F.count(o_c.C_ACCTBAL),F.sum(o_c.C_ACCTBAL))
    .sort(o_c.COUNTRY_CODE)

o_c_cavg.show()












