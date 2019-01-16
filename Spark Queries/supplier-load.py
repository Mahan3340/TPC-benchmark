from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/supplier.tbl")

fields=[StructField("S_SUPPKEY", IntegerType(), True)
        , StructField("S_NAME", StringType(),True),
        StructField("S_ADDRESS", StringType(),True),
        StructField("S_NATIONKEY", IntegerType(),True),
        StructField("S_PHONE", StringType(),True),
        StructField("S_ACCTBAL", FloatType(),True),
        StructField("S_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'S_SUPPKEY':int(x[0]),
        'S_NAME':x[1],
        'S_ADDRESS':x[2],
        'S_NATIONKEY': int(x[3]),
        'S_PHONE':x[4],
        'S_ACCTBAL':float(x[5]),
        'S_COMMENT':x[6],
        })\
    .toDF(schema)

df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/supplier.parquet")
