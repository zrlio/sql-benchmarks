## Spark SQL Benchmarks
A set of SQL benchmarks for Spark 

### How to build 

Clone the source repo and execute maven build.

### How to run
 
 ```bash
  ./bin/spark-submit --master yarn \ 
  --class com.ibm.crail.benchmarks.Main \ 
  sql-benchmarks-1.0.jar [OPTIONS]
 ```
 
 Current options are : 
 
 ```
 usage: Main
  -a,--action <arg>   action to take. Your options are:
                      1. count (default)
                      2. collect, items[int, default: 100]
                      3. save, filename[str, default: /tmp], format[str, default: parquet]
  -h,--help           show help.
  -i,--input <arg>    comma separated list of input files/directories.
                      EquiJoin takes two files and q65 takes a tpc-ds data
                      directory
  -k,--key <arg>      key for EquiJoin, default is IntIndex
  -t,--test <arg>     which test to perform, options are: equijoin or q65
  -v,--verbose        verbose

```

`test (-t)` can either execute an EquiJoin (default) or Query65 (q65). For EquiJoin, the key to join on is specified by 
 `-k` option. The default is `intKey`. If you generated data using ParquetGenerator, this key should be present. 
 `-i` options set ups the input files. EquiJoin takes two comma separated list of files. Query65 takes a directory 
  where the TPC-DS data is present. `Action (-a)` tells how the `test` should be executed. There are currently three
   options. 
   * Count: call `count` on the result Dataset RDD. The format for this option is : `-a count`
   * Collect : call `limit(items)` and then `collect` on the result Dataset RDD. The format for this option is : 
    `-a collect,101`
   * Save: save the result Dataset RDD to a file in a specific format. The format for this option is : 
   ` -a save,filename,format`

### Example runs 

#### Executing join on two tables generated from the ParquetGenerator
The action here is saving the result as a parquet file at `/data/tmp`
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar\
  -a save,/data/tmp,parquet -i /data/sql/f1,/data/sql/f2/
``` 
#### Executing query65 
The action here is collecting the top 105 elements  
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar \
 -t q65 -a collect,105  -i /data/sql/pg-tpcds-F1/
```