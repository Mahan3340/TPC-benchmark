from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/part.tbl")

fields=[
        StructField("P_PARTKEY", IntegerType(), True),
        StructField("P_NAME", StringType(),True),
        StructField("P_MFGR", StringType(),True),
        StructField("P_BRAND", StringType(),True),
        StructField("P_TYPE", StringType(),True),
        StructField("P_SIZE", IntegerType(),True),
        StructField("P_CONTAINER", StringType(),True),
        StructField("P_RETAILPRICE", FloatType(),True),
        StructField("P_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'P_PARTKEY':int(x[0]),
        'P_NAME':x[1],
        'P_MFGR':x[2],
        'P_BRAND':x[3],
        'P_TYPE':x[4],
        'P_SIZE':int(x[5]),
        'P_CONTAINER':x[6],
        'P_RETAILPRICE':float(x[7]),
        'P_COMMENT':x[8],
        })\
    .toDF(schema)


df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/part.parquet")
