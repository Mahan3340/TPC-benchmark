from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")

fields=[
        StructField("N_NATIONKEY", IntegerType(), True),
        StructField("N_NAME", StringType(), True),
        StructField("N_REGIONKEY", IntegerType(), True),
        StructField("N_COMMENT", StringType(), True)]

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'N_NATIONKEY':int(x[0]),
        'N_NAME':x[1],
        'N_REGIONKEY':int(x[2]),
        'N_COMMENT':x[3]})\
    .toDF(schema)

schema=StructType(fields)

df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/nation.parquet")
