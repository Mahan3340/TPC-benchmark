from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

sqlContext.setConf('spark.sql.orc.impl', 'native')

nation.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/nation.orc")
region.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/region.orc")
supplier.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/supplier.orc")
customer.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/customer.orc")
part.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/part.orc")
partsupp.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/partsupp.orc")
orders.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/orders.orc")
lineitem.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/mahan-data/lineitem.orc")


