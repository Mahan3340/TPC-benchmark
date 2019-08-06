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

l_temp = lineitem
    .groupBy('L_ORDERKEY')
    .agg(F.sum('L_QUANTITY').alias("SUM_QUANTITY"))
    .filter('sum_quantity > 300')
    .select(lineitem.L_ORDERKEY.alias("KEY"),'SUM_QUANTITY')

l_o = l_temp
    .join(orders,l_temp.KEY == orders.O_ORDERKEY)

l_o = l_o
    .join(lineitem,lineitem.L_ORDERKEY == l_o.KEY)

l_o_c = l_o
    .join(customer,customer.C_CUSTKEY == l_o.O_CUSTKEY)
    .select('L_QUANTITY','C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE','O_TOTALPRICE')
    .groupBy('C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE','O_TOTALPRICE')
    .agg(F.sum('L_QUANTITY'))
    .sort(F.desc("O_TOTALPRICE"),'O_ORDERDATE')

l_o_c.limit(100).show()


