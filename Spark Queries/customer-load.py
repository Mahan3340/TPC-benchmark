from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F


rdd = sc.textFile("/data/OLAP_Benchmark_data/customer.tbl")

fields=[StructField("C_CUSTKEY", IntegerType(), True),
        StructField("C_NAME", StringType(),True),
        StructField("C_ADDRESS", StringType(),True),
        StructField("C_NATIONKEY", IntegerType(),True),
        StructField("C_PHONE", StringType(),True),
        StructField("C_ACCTBAL", FloatType(),True),
        StructField("C_MKTSEGMENT", StringType(),True),
        StructField("C_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'C_CUSTKEY':int(x[0]),
        'C_NAME':x[1],
        'C_ADDRESS':x[2],
        'C_NATIONKEY':int(x[3]),
        'C_PHONE':x[4],
        'C_ACCTBAL':float(x[5]),
        'C_MKTSEGMENT':x[6],
        'C_COMMENT':x[7]
        })\
    .toDF(schema)


df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/customer.parquet")

