# !/usr/bin/bash

# DTAT420 Assignment 1
# Analysis Q3(a)

# check the default blocksiez
hdfs getconf -confkey "dfs.blocksize"

# check the file size for 2010 and 2017
hdfs dfs -du -h hdfs:///data/ghcnd/daily/2017.csv.gz
hdfs dfs -du -h hdfs:///data/ghcnd/daily/2010.csv.gz

# check the detail of 2010 and 2017
hdfs fsck hdfs:///data/ghcnd/daily/2017.csv.gz -blocks
hdfs fsck hdfs:///data/ghcnd/daily/2010.csv.gz -blocks
