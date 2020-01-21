###### DATA420 Assignment 2 Peng Shen (student ID: 57408055)
##### Processing

####  Q1(a)
# Check how the data is structured 
!hdfs dfs -ls -R /data/msd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

#### Q1(c)
#The following loads and counts the number of rows in each of the datasets,
#and the results are presented in the following tables. Note that some of
#these rows will likely be removed when the datasets are joined, as not all
#songs exist in every dataset provided.
start_pyspark_shell -e 2 -c 1 -w 1 -m 1
 
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pretty import SparkPretty  # download pretty.py from LEARN and put in the same directory

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
pretty = SparkPretty(limit=5)


# Check number of rows in audio dataset
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv | wc -l #21
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv | wc -l #21
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv | wc -l #11
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv | wc -l #27
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv | wc -l #17
!hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv | wc -l #17
!hdfs dfs -cat /data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv | wc -l #125
!hdfs dfs -cat /data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv | wc -l #421
!hdfs dfs -cat /data/msd/audio/attributes/msd-rh-v1.0.attributes.csv | wc -l #61
!hdfs dfs -cat /data/msd/audio/attributes/msd-rp-v1.0.attributes.csv | wc -l #1441
!hdfs dfs -cat /data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv | wc -l #169
!hdfs dfs -cat /data/msd/audio/attributes/msd-trh-v1.0.attributes.csv | wc -l #421
!hdfs dfs -cat /data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv | wc -l #1177

!hdfs dfs -cat /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/* | gunzip | wc -l  #994623
!hdfs dfs -cat /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/* | gunzip | wc -l  #995001
!hdfs dfs -cat /data/msd/audio/features/msd-mvd-v1.0.csv/* | gunzip | wc -l  #994188
!hdfs dfs -cat /data/msd/audio/features/msd-rh-v1.0.csv/* | gunzip | wc -l  #994188
!hdfs dfs -cat /data/msd/audio/features/msd-rp-v1.0.csv/* | gunzip | wc -l  #994188
!hdfs dfs -cat /data/msd/audio/features/msd-ssd-v1.0.csv/* | gunzip | wc -l  #994188
!hdfs dfs -cat /data/msd/audio/features/msd-trh-v1.0.csv/* | gunzip | wc -l #994188
!hdfs dfs -cat /data/msd/audio/features/msd-tssd-v1.0.csv/* | gunzip | wc -l #994188

!hdfs dfs -cat /data/msd/audio/statistics/* | gunzip | wc -l #992866

# Check number of rows in genre dataset
!hdfs dfs -cat /data/msd/genre/msd-MAGD-genreAssignment.tsv | wc -l #422714
!hdfs dfs -cat /data/msd/genre/msd-MASD-styleAssignment.tsv | wc -l #273936
!hdfs dfs -cat /data/msd/genre/msd-topMAGD-genreAssignment.tsv | wc -l #406427

# Check number of rows in main datasets
!hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | wc -l #1000001
!hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | wc -l #1000001

# Check number of rows in tasteprofile datasets
!hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | wc -l #938
!hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | wc -l #19094

!hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/* | gunzip | wc -l #48373586

