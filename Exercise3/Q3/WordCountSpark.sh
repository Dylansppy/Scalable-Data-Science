#!/usr/bin/bash
# Run pyspark shell interactively

NAME=psh99

# WordCount.py

submit_pyspark_script "WordCount.py hdfs:///user/$NAME/helloworld hdfs:///user/$NAME/word_count_spark"
hdfs dfs -ls /user/$NAME/word_count_spark/
hdfs dfs -ls /user/$NAME/word_count_spark/ | sed -n "s/^.* [1-9][0-9]* 2019.* [^\/]*\(\/.*$\)/\1/p" | parallel "echo {}; hdfs dfs -cat {}"
