#!/usr/bin/bash

# Processing

# Q1(a)

# Explore ghcnd directory
hdfs dfs -ls /data/ghcnd

# Print file tree
hdfs dfs -ls -R /data/ghcnd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

# Q1(b)

# Explore files under daily directory
hdfs dfs -ls /data/ghcnd/daily | wc -l

# Q1(c)

# Check the size of all the data under ghcnd
hdfs dfs -du /data # Unit Byte
hdfs dfs -du -h /data # Unit Mb, Gb

# Check the size of daily
hdfs dfs -du /data/ghcnd # Unit Byte
hdfs dfs -du -h /data/ghcnd # Unit Mb, Gb

#Q2

# Explore the schema of data
hdfs dfs -cat /data/ghcnd/daily/2017.csv.gz | gunzip | head
hdfs dfs -cat /data/ghcnd/stations | head
hdfs dfs -cat /data/ghcnd/countries | head
hdfs dfs -cat /data/ghcnd/states | head
hdfs dfs -cat /data/ghcnd/inventory | head