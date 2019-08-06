#!/usr/bin/bash
 
# Run pig script on Hadoop

NAME=psh99

# WordCount.pig

pig -x mapreduce -param INPUT="/user/$NAME/helloworld" -param OUTPUT="/user/$NAME/word_count_pig" WordCount.pig
hdfs dfs -ls /user/$NAME/word_count_pig
hdfs dfs -cat /user/$NAME/word_count_pig/part-r-00000
