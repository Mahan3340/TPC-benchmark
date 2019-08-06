from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/lineitem.tbl")

fields=[
        StructField("L_ORDERKEY", IntegerType(), True),
        StructField("L_PARTKEY", IntegerType(),True),
        StructField("L_SUPPKEY", IntegerType(),True),
        StructField("L_LINENUMBER", IntegerType(),True),
        StructField("L_QUANTITY", FloatType(),True),
        StructField("L_EXTENDEDPRICE", FloatType(),True),
        StructField("L_DISCOUNT", FloatType(),True),
        StructField("L_TAX", FloatType(),True),
        StructField("L_RETURNFLAG", StringType(),True),
        StructField("L_LINESTATUS", StringType(),True),
        StructField("L_SHIPDATE", StringType(),True),
        StructField("L_COMMITDATE", StringType(),True),
        StructField("L_RECEIPTDATE", StringType(),True),
        StructField("L_SHIPINSTRUCT", StringType(),True),
        StructField("L_SHIPMODE", StringType(),True),
        StructField("L_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'L_ORDERKEY':int(x[0]),
        'L_PARTKEY': int(x[1]),
        'L_SUPPKEY': int(x[2]),
        'L_LINENUMBER': int(x[3]),
        'L_QUANTITY':float(x[4]),
        'L_EXTENDEDPRICE':float(x[5]),
        'L_DISCOUNT':float(x[6]),
        'L_TAX':float(x[7]),
        'L_RETURNFLAG':x[8],
        'L_LINESTATUS':x[9],
        'L_SHIPDATE':x[10],
        'L_COMMITDATE':x[11],
        'L_RECEIPTDATE':x[12],
        'L_SHIPINSTRUCT':x[13],
        'L_SHIPMODE':x[14],
        'L_COMMENT':x[15]
        })\
    .toDF(schema)


df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/lineitem.parquet")

