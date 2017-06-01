# Spark SQL Benchmarks
A set of Spark SQL benchmarks for humans. 

## How to build 
```bash
mvn -DskipTests -T 1C install
```
This should give you `sql-benchmarks-1.0.jar` in your `target` folder.
To build for with-dependencies, you can use: 
```bash
mvn -DskipTests -T 1C clean compile assembly:single
```

## How to run
 
 ```bash
  ./bin/spark-submit --master yarn \ 
  --class com.ibm.crail.benchmarks.Main \ 
  sql-benchmarks-1.0.jar [OPTIONS]
 ```
 
 Current options are : 
  
 ```
 usage: Main
  -a,--action <arg>        action to take. An action is an operation in Spark that triggers the computation*. 
                            Your options are:
                            1. count (default)
                            2. collect,items[int, default: 100]
                            3. save,filename[str, default: /tmp],format[str, default: parquet]
  -h,--help                show help.
  -i,--input <arg>         comma separated list of input files/directories.
                            EquiJoin takes two files.
                            Q65 takes a TPC-DS data directory.
                            ReadOnly takes one file. 
  -k,--key <arg>           the join key for the EquiJoin (default: IntIndex)$ 
  -t,--test <arg>          which test to perform, options are (case insensitive): equiJoin, q65, readOnly  
  -v,--verbose             verbose
  -w,--warmupInput <arg>   warmup files, same semantics as the -i
```

 `*`https://spark.apache.org/docs/latest/programming-guide.html#actions
 
 `$` If you generate data from the parquet generator tool, then its schemas have a column name called `IntIndex` 
   
`test (-t)` can either execute an EquiJoin (default), readOnly, or the quert 65 (q65) from the TPC-DS test suit. For 
EquiJoin, the key to join on is specified by `-k` option. The default is `intKey`. If you generated data using 
ParquetGenerator, this key should be present. `-i` options set ups the input files. EquiJoin takes two comma separated 
list of files. Query65 takes a directory where the TPC-DS data is present. ReadOnly test takes one file. 

`Action (-a)` tells how the `test` should be executed. There are currently three options.  
   * Count: call `count` on the result Dataset RDD. The format for this option is : `-a count`
   * Collect : call `limit(items)` and then `collect` on the result Dataset RDD. To collect 101 items: 
    `-a collect,101`
   * Save: save the result Dataset RDD to a file in a specific format. The format for this option is : 
   ` -a save,filename,format` 

[WARNING: No space between the arguments and commas]

`WarmUp (-w)` does the same action (-a) as intended for the test command (-t) but on different input files. Its
semantics are the same as the input files. It is meant to JIT the java code path and setup resources, if any. We 
recommend to use different files than the actual input files (-i) for warmup run to avoid any data/metadata caching 
issues. 

## Example runs 

### Executing EquiJoin with save
Executing join on two tables generated from the ParquetGenerator and save the output as a parquet file at `/data/tmp`.  
The action here is saving the result.
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar\
  -a save,/data/tmp,parquet -i /data/sql/f1,/data/sql/f2/
``` 
### Executing query65 with collect
The action here is collecting the top 105 elements  
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar \
 -t q65 -a collect,105  -i /data/sql/tpcds/
```
### Executing readOnly with count and WarmUp
 The action here is count. 
 Due to warm up the whole test and action will be first executed on the warmup (`/data/sql/warmup.parquet`) file.  
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar \
 -t readOnly -a count -i /data/largeFile.parquet -w /data/sql/warmup.parquet
```

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com