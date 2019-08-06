# Compile code

mkdir ~/build
hadoop com.sun.tools.javac.Main ~/WordCount.java
jar cf ~/WordCount.jar *.class
rm -rf *.class

# Run compiled jar using Hadoop MapReduce

NAME=abc123

hadoop jar ~/WordCount.jar WordCount /user/$NAME/helloworld/ /user/$NAME/word_count_hadoop/
hdfs dfs -ls /user/$NAME/word_count_hadoop/
hdfs dfs -cat /user/$NAME/word_count_hadoop/part-r-00000
