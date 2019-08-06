# List files and directories in HDFS

NAME=abc123

hdfs dfs -ls /data/  # shared data directory
hdfs dfs -ls /user/
hdfs dfs -ls /user/$NAME/  # your user directory

# Copy data already in HDFS to your user directory

hdfs dfs -cp /data/helloworld/ /user/$NAME/
hdfs dfs -ls /user/$NAME/helloworld/
hdfs dfs -cat /user/$NAME/helloworld/part-i-00000

# Copy data from HDFS

hdfs dfs -copyToLocal /user/$NAME/helloworld/part-i-00000 ~/part-i-00000
cat ~/part-i-00000

# Delete data from HDFS

hdfs dfs -rm -r /user/$NAME/helloworld/

# Copy data to HDFS

hdfs dfs -copyFromLocal /scratch-network/courses/2019/DATA420-19S1/data/helloworld/ /user/$NAME/
hdfs dfs -ls /user/$NAME/helloworld/
hdfs dfs -cat /user/$NAME/helloworld/part-i-00000

# Detailed report of data in HDFS

hdfs fsck /user/$NAME/helloworld/part-i-00000 -files -blocks -locations
