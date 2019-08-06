from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/partsupp.tbl")

fields=[
        StructField("PS_PARTKEY", IntegerType(), True),
        StructField("PS_SUPPKEY", IntegerType(),True),
        StructField("PS_AVAILQTY", IntegerType(),True),
        StructField("PS_SUPPLYCOST", FloatType(),True),
        StructField("PS_COMMENT", StringType(),True)]
schema=StrructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'PS_PARTKEY':int(x[0]),
        'PS_SUPPKEY':int(x[1]),
        'PS_AVAILQTY':int(x[2]),
        'PS_SUPPLYCOST':float(x[3]),
        'PS_COMMENT':x[4]
        })\
    .toDF(schema)


df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/partsupp.parquet")

