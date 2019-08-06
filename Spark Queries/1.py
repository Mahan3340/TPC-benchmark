from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F
import datetime

lineitem = sqlContext.read.parquet("hdfs://namenode:8020/mahan-data/lineitem.parquet")

res = lineitem.filter("l_shipdate <= '1998-09-02'")
    .groupBy('l_returnflag','l_linestatus')
    .agg(
         F.sum('l_quantity').alias('sum_qty'),
         F.sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)).alias('sum_disc_price'),
         F.sum('l_extendedprice').alias('sum_base_price'),
         F.sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)*(1+lineitem.L_TAX)).alias('sum_charge'),
         F.avg('l_quantity').alias('avg_qty'),
         F.avg('l_extendedprice').alias('avg_price'), F.avg('l_discount').alias('avg_disc'))
    .sort('l_returnflag','l_linestatus')

res.show()

