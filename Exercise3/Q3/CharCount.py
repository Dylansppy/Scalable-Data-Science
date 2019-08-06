# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Char counts

data_path = sys.argv[1]
char_counts_path = sys.argv[2]

data = sc.textFile(data_path)
char_counts = data \
    .flatMap(lambda line: line.split(" ")) \
    .flatMap(lambda word: list(word)) \
    .map(lambda char: (char, 1)) \
    .reduceByKey(lambda x, y: x + y)

char_counts.saveAsTextFile(char_counts_path)
