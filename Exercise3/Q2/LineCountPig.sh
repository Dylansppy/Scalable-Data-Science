#!/usr/bin/bash
 
# Run pig script on Hadoop

NAME=psh99

# WordCount.pig

pig -x mapreduce -param INPUT="/user/$NAME/helloworld" -param OUTPUT="/user/$NAME/line_count_pig" LineCount.pig
hdfs dfs -ls /user/$NAME/line_count_pig
hdfs dfs -cat /user/$NAME/line_count_pig/part-r-00000
