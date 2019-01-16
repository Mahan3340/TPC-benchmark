from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

rdd = sc.textFile("/data/OLAP_Benchmark_data/orders.tbl")

fields=[StructField("O_ORDERKEY", IntegerType(), True),
        StructField("O_CUSTKEY", IntegerType(),True),
        StructField("O_ORDERSTATUS", StringType(),True),
        StructField("O_TOTALPRICE", FloatType(),True),
        StructField("O_ORDERDATE", DateType(),True),
        StructField("O_ORDERPRIORITY", StringType(),True),
        StructField("O_CLERK", StringType(),True),
        StructField("O_SHIPPRIORITY", IntegerType(),True),
        StructField("O_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'O_ORDERKEY':int(x[0]),
        'O_CUSTKEY':int(x[1]),
        'O_ORDERSTATUS':x[2],
        'O_TOTALPRICE':float(x[3]),
        'O_ORDERDATE':x[4],
        'O_ORDERPRIORITY':x[5],
        'O_CLERK':x[6],
        'O_SHIPPRIORITY':int(x[7]),
        'O_COMMENT':x[8],
        })\
    .toDF(schema)

df.write.mode("overwrite").parquet("hdfs://namenode:8020/mahan-data/orders.parquet")
