# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Word counts

data_path = sys.argv[1]
word_counts_path = sys.argv[2]

data = sc.textFile(data_path)
word_counts = data \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)

word_counts.saveAsTextFile(word_counts_path)
