# TPC-benchmark
TPC-H Benchmark on Spark (Avro,Parquet,ORC formats) , PostgreSQL , Flink (Avro Format)

### PostgreSQL 

##### Load Data into Tables :

```
 BEGIN; \COPY tableName from filePath WITH DELIMITER AS 'yourDataDelimiter' COMMIT;
 ```

##### Run Queries:

```
 \o outputfile.txt 
 \i query.sql
 ```
### Spark

- For Parquet format Load data into tables via tableName-load.py files in ```/Spark Queries/``` folder
- Same for avro and orc, except, add /Jars/Spark files into Spark ```.../jars/``` folder for both Spark Master and all Spark Workrs
- Run Queries from ```/Spark Queries/```

### Flink 

- Copy all jar files from ```/Jars/Flink``` into ```Flink/Lib``` folder into flink-taskmanager and flink-jobManager lib folders

** Note that all the jars included in this folder should be the same version as the one that your current Flink supports
	I used Flink 1.6.2 and all the jar files are compatile with version 1.6.2.
	
** If you want to compile and debug (till right before actually running the query, to check syntax & ,...)  on your                         Local Machine like me, be sure to use the same version of flink-client and other jars uploaded to the server to avoid java serialization errors due to UUID changes.

** You can use ```mvn package``` (Maven) command to create light weight jars to avoid uploading large files, or else you have to  make fat jars to include all your dependencies.

- To use my project's final jar version run the queries via the lastest jar as below after you copied the jar to your server
- cd to the jar's location
- ``` Flink run -c jobs.Q# (replace # with the a TPC-H query number fomr 1 to 22) jarfileName.jar ```
  for e.g: ```Flink run -c jobs.Q5 tpcc-flink.jar```
- This will ask you to provide some Paths:

a) For my convinient, I managed to save all texts printed on the console to a file, regular one's and exceptions on seprate files so the first address you provide is the directory where these two files will be saved as for Query Above, if you enter the address ```/results/``` the files will be saved on this folder as : ```Q5-out.txt and Q5-error.txt``` , which the former contains all the regular System.out.prints and the latter contains exceptions if any have occured, if it's empty, then the Query has been completed successfully and the results are saved somewhere. 

** as for this path be sure to include '/' at both ends and be sure that the folders under the directory alread exist I wont check for them.

b) The second address that you should provide is the one needed to write the query results to, this path unfortunately should be HDFS only, I didn't spend more time to figure the problem with saving it on local machine
so the path would be something like ```hdfs://namenode:8020/your-path/```  and the results would be accessible on 
```hdfs://namenode/your-path/Q5-res.csv```

** as for this path be sure to include '/' at both ends and be sure that the folders under the directory alread exist I wont check for them.

** Be aware that the output file of the query will be overwritten if it alreay exits, be careful if you are running the same query again !

c) The third and the last address that you will be asked for is the directory that all the tables are accessible in avro format, this address will also needed to be a HDFS path, for example ```hdfs://namenode:8020/my-data/```

** as for this path be sure to include '/' at both ends
** this directory should include all the tables with avro format with names like below:

```
nation.avro
lineitem.avro
part.avro
partsupp.avro
supplier.avro
order.avro
customer.avro
region.avro
```

d) some of the Queries will need more resources to be executed successfully, here's my modifications to the configuration file under /Flink/Conf file for a Server with 16 Cores and 39GB of Ram :

```
#heap size should be less than memory size

taskmanager.heap.size: 8192m

taskmanager.memory.size: 16384m

taskmanager.memory.segment-size: 64kb

taskmanager.memory.fraction: 0.8
 
 #if a job fails due to some reasons, it will re execute the query again after a fixed amount of time
 
 restart-strategy: fixed-delay
 
 parallelism.default: 1
 
 ```






