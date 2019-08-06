from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

orders = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/orders.avro")
lineitem = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/lineitem.avro")
nation = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/nation.avro")
region = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/region.avro")
part = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/part.avro")
supplier = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/supplier.avro")
partsupp = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/partsupp.avro")
customer = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/mahan-data/customer.avro")

