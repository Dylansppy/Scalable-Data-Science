#!/usr/bin/bash

# Compile code

mkdir ~/build
hadoop com.sun.tools.javac.Main ~/LineCount.java
jar cf ~/LineCount.jar *.class
rm -rf *.class

# Run compiled jar using Hadoop MapReduce

NAME=psh99

hadoop jar ~/LineCount.jar LineCount /user/$NAME/helloworld/ /user/$NAME/line_count_hadoop/
hdfs dfs -ls /user/$NAME/line_count_hadoop/
hdfs dfs -cat /user/$NAME/line_count_hadoop/part-r-00000
 