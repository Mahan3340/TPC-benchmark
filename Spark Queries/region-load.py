from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'R_REGIONKEY':int(x[0]),
        'R_NAME':x[1],
        'R_COMMENT':x[2]})\
    .toDF(schema)

fields=[StructField("R_REGIONKEY", IntegerType(), True),
        StructField("R_NAME", StringType(),True),
        StructField("R_COMMENT", StringType(),True)]

schema=StructType(fields)


df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/region.parquet")

