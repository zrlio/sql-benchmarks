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
   -a,--action <arg>                  action to take. Your options are
                                      (important, no space between ','):
                                      1. count (default)
                                      2. collect,items[int, default: 100]
                                      3. save,filename[str, default: /tmp]
   -gi,--graphPRIterations <arg>      number of iteration for the PageRank
                                      algorithm, default 8
   -h,--help                          show help.
   -i,--input <arg>                   comma separated list of input
                                      files/directories. EquiJoin takes two
                                      files, TPCDS queries takes a tpc-ds
                                      data directory, and readOnly take a
                                      file or a directory with files
   -if,--inputFormat <arg>            input format (where-ever applicable)
                                      default: parquet
   -ifo,--inputFormatOptions <arg>    input format options as
                                      key0,value0,key1,value1...
   -k,--key <arg>                     key for EquiJoin, default is IntIndex
   -of,--outputFormat <arg>           output format (where-ever applicable)
                                      default: parquet
   -ofo,--outputFormatOptions <arg>   output format options as
                                      key0,value0,key1,value1...
   -t,--test <arg>                    which test to perform, options are
                                      (case insensitive): equiJoin,
                                      qXXX(tpcds queries), tpcds, readOnly
   -v,--verbose                       verbose
   -w,--warmupInput <arg>             warmup files, same semantics as the -i
```

 `*`https://spark.apache.org/docs/latest/programming-guide.html#actions
 
 `$` If you generate data from the parquet generator tool, then its schemas have a column name called `IntIndex` 
   
`test (-t)` can either execute an EquiJoin (default), readOnly, or the whole (tpcds) or a specific query from the 
TPC-DS test suit. For EquiJoin, the key to join on is specified by `-k` option. The default is `intKey`. If you 
generated data using ParquetGenerator, this key should be present. `-i` options set ups the input files. EquiJoin 
takes two comma separated list of files. TPC-DS tests takes the directory locaiton containing dataset. 
ReadOnly test takes one or more files (with the same schema!). 

`Action (-a)` tells how the `test` should be executed. There are currently three options.  
   * Count: call `count` on the result Dataset RDD. The format for this option is : `-a count`
   * Collect : call `limit(items)` and then `collect` on the result Dataset RDD. To collect 101 items: 
    `-a collect,101`
   * Save: save the result Dataset RDD to a file in a specific format (say csv). The format for this option is : 
   ` -a save,filename -of csv`  

[WARNING: No space between the arguments and commas]

`WarmUp (-w)` does the same action (-a) as intended for the test command (`-t`) but on different input files. Its
semantics are the same as the input files. It is meant to JIT the java code path and setup resources, if any. We 
recommend to use different files than the actual input files (`-i`) for warm-up run to avoid any data/metadata caching 
issues. 

### Input and Output formats and options 

The input format is controller by specifying `-if` parameter. Additional format specific parameters can be passed using
`-ifo` parameter. The default input format is parquet without any specific options.  

The output format for `-a save` action is controller by specifying `-of` parameter. Additional format specific 
parameters can be passed using `-ofo` parameter. The default output format is parquet with compression disabled. 
For example, if you want to enable compression for the save action you can pass `-ofo compression,gzip`. This 
 will enable `gzip` compression for parquet. 
 
## Example runs 

### Executing EquiJoin with save
Executing join on two tables generated from the ParquetGenerator and save the output as a parquet with snappy compression
file at `/data/tmp`. The action here is saving the result.
```bash
 ./bin/spark-submit -v --num-executors 4 --executor-cores 1 --executor-memory 1G --driver-memory 4G \
 --master local \
 --class com.ibm.crail.benchmarks.Main ./apps/sql-benchmarks-1.0.jar\
  -a save,/data/tmp -i /data/sql/f1,/data/sql/f2/ -of parquet -ofo compression,snappy  
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
#### Executing the whole tpc-ds benchmark with warmup and save output
 This example is for local Spark execution, and we save the output in the parquet format
```bash
./bin/spark-submit --master local  --num-executors 2 --executor-cores 2 --executor-memory 1g \
--driver-memory 1g --class com.ibm.crail.benchmarks.Main \
~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
-t tpcds \
-i crail://localhost:9060/tpcds/ \
-a save,crail://localhost:9060/tpcds-output/ \
-of parquet  \
-w crail://localhost:9060/warmup-tpcds/
```

#### Running 32 iteration of pagerank on an input graph 
```bash
./bin/spark-submit -v --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g \
--driver-cores 2 --master local \
--class com.ibm.crail.benchmarks.Main \
~/jars/sql-benchmarks-1.0.jar \
-t pagerank -gi 2 -i /soc-LiveJournal1.txt
```

output as 

```

-------------------------------------------------
Test           : PageRank 2 iterations on /soc-LiveJournal1.txt
Action         : No-Op (no explicit action was necessary)
Execution time : 18563 msec
Result         : Ran PageRank 2 iterations /soc-LiveJournal1.txt
---------------- Additional Info ------------------
Graph load time: 18643 msec
-------------------------------------------------
```

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com