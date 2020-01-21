###### DATA420 Assignment 2 Peng Shen (student ID: 57408055)
##### Song Recommendations

# Python and pyspark modules required
start_pyspark_shell -e 4 -c 4 -w 8 -m 8

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import Window
from operator import or_
from functools import reduce
from pretty import SparkPretty  # download pretty.py from LEARN and put in the same directory

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
pretty = SparkPretty(limit=5)

# Compute suitable number of partitions
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

# --------------------------------------------------------------
# ---------------------------Q1-------------------------------
# --------------------------------------------------------------

#  Q1 (a)
# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------
mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2019/DATA420-19S2/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 40)

print(matches_manually_accepted.count())  # 488

with open("/scratch-network/courses/2019/DATA420-19S2/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 40)

print(mismatches.count())  # 19094

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
triplets.cache()
triplets.show(10, 50)

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

print(mismatches_not_accepted.count())  # 19093
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111

# How many unique songs?
triplets_not_mismatched.dropDuplicates(['song_id']).count() #378310

# How many unique users?
triplets_not_mismatched.dropDuplicates(['user_id']).count() #1019318

# --------------------------------------------------------------
# Q1 (b)
def get_user_counts(triplets):
    return (
        triplets
        .groupBy("user_id")
        .agg(
            F.count(col("song_id")).alias("song_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )
    
# User statistics
user_counts = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.count(col("song_id")).alias("song_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
user_counts.cache()
user_counts.count()   # 1019318

user_counts.show(10, False)

# +----------------------------------------+----------+----------+
# |user_id                                 |song_count|play_count|
# +----------------------------------------+----------+----------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195       |13074     |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362      |9104      |
# |3fa44653315697f42410a30cb766a4eb102080bb|146       |8025      |
# |a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|518       |6506      |
# |d7d2d888ae04d16e994d6964214a1de81392ee04|1257      |6190      |
# |4ae01afa8f2430ea0704d502bc7b57fb52164882|453       |6153      |
# |b7c24f770be6b802805ac0e2106624a517643c17|1364      |5827      |
# |113255a012b2affeab62607563d03fbdf31b08e7|1096      |5471      |
# |99ac3d883681e21ea68071019dba828ce76fe94d|939       |5385      |
# |6d625c6557df84b60d90426c0116138b617b9449|1307      |5362      |
# +----------------------------------------+----------+----------+

statistics = (
    user_counts
    .select("song_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#               count                mean              stddev min    max
# song_count  1019318   44.92720721109605   54.91113199747355   3   4316
# play_count  1019318  128.82423149596102  175.43956510304616   3  13074

user_counts.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
user_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# [3.0, 20.0, 32.0,  58.0,  4316.0]
# [3.0, 35.0, 71.0, 173.0, 13074.0]

# How many different songs has the most active user played?
# 4316 
# 4316/378310 = 1.14%

# --------------------------------------------------------------
# Q1 (c)
def get_song_counts(triplets):
    return (
        triplets
        .groupBy("song_id")
        .agg(
            F.count(col("user_id")).alias("user_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )
    
# Song statistics
song_counts = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.count(col("user_id")).alias("user_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
song_counts.cache()
song_counts.count() # 378310

song_counts.show(10, False)

# +------------------+----------+----------+
# |song_id           |user_count|play_count|
# +------------------+----------+----------+
# |SOBONKR12A58A7A7E0|84000     |726885    |
# |SOSXLTC12AF72A7F54|80656     |527893    |
# |SOEGIYH12A6D4FC0E3|69487     |389880    |
# |SOAXGDH12A8C13F8A1|90444     |356533    |
# |SONYKOW12AB01849C9|78353     |292642    |
# |SOPUCYA12A8C13A694|46078     |274627    |
# |SOUFTBI12AB0183F65|37642     |268353    |
# |SOVDSJC12A58A7A271|36976     |244730    |
# |SOOFYTN12A6D4F9B35|40403     |241669    |
# |SOHTKMO12AB01843B0|46077     |236494    |
# +------------------+----------+----------+

statistics = (
    song_counts
    .select("user_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#              count                mean             stddev min     max
# user_count  378310  121.05181200602681  748.6489783736941   1   90444
# play_count  378310   347.1038513388491  2978.605348838212   1  726885

song_counts.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
song_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# [1.0,  5.0, 13.0, 104.0,  90444.0]
# [1.0, 11.0, 36.0, 217.0, 726885.0]

# Plot distribution of song popularity (x=user_count, y=count)
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

popularity_distribution = (
    song_counts.
    select('user_count')
    .toPandas()
    )
    
# Plot distribution of user activity (x=song_count, y=count)
activity_distribution = (
    user_counts.
    select('song_count')
    .toPandas()
    )

def distribution_hist(name, df, x):
    plt.figure(dpi=300, figsize=(10, 10))
    plt.title(f'Distribution of {name}', fontsize=30)
    plt.hist(df[x])
    plt.ylabel('Frequency', fontsize = 20, rotation=90)
    plt.xlabel(x, fontsize = 20)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.savefig(f'{name}_distribution.jpg')


distribution_hist('Popularity', popularity_distribution, 'user_count')
distribution_hist('Activity', activity_distribution, 'song_count')

# Cumulative distribution function (CDF) plot
def distribution_ecdf(name, df, col):
    x = np.sort(df[col])
    y = np.arange(1, len(x)+1) / len(x)
    plt.clf()
    plt.figure(dpi=300, figsize=(12, 4))
    plt.plot(x, y, marker = '.', linestyle='none')
    plt.title(f'Distribution of {name}', fontsize=20)
    plt.ylabel('ECDF', fontsize = 12, rotation=90)
    plt.xlabel(col, fontsize = 12)
    plt.yticks(fontsize=10)
    plt.xticks(fontsize=10)
    plt.xlim((0, 600))
    plt.savefig(f'{name}_ecdf.jpg')

distribution_ecdf('Song Popularity', popularity_distribution, 'user_count')
distribution_ecdf('User Activity', activity_distribution, 'song_count')

# --------------------------------------------------------------
# Q1 (d)
# -----------------------------------------------------------------------------
# Limiting
# -----------------------------------------------------------------------------

user_song_count_threshold = 10
song_user_count_threshold = 5

triplets_limited = triplets_not_mismatched

i = 0
for i in range(0, 5): 
    triplets_limited = (
        triplets_limited
        .join(
            triplets_limited.groupBy("user_id").count().where(col("count") > user_song_count_threshold).select("user_id"),
            on="user_id",
            how="inner"
        )
        .join(
            triplets_limited.groupBy("song_id").count().where(col("count") > song_user_count_threshold).select("song_id"),
            on="song_id",
            how="inner"
        )
    )
    triplets_limited.cache()
    triplets_limited.count()
    i = i + 1
    
triplets_limited.count() #  44697407
triplets_limited.count() / triplets_not_mismatched.count() # 44697407/45795111 = 0.9760
print(get_user_counts(triplets_limited).approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)) # min > 10
print(get_song_counts(triplets_limited).approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)) # min > 5
# [10.0, 18.0, 32.0, 87.0, 4068.0]
# [6.0, 13.0, 32.0, 108.0, 86610.0]

(
    triplets_limited
    .agg(
        countDistinct(col("user_id")).alias('user_count'),
        countDistinct(col("song_id")).alias('song_count')
    )
    .toPandas()
    .T
    .rename(columns={0: "value"})
)

#              value
# user_count  936546 / 1019318 = 0.9188
# song_count  262037 /  378310 = 0.6927


# Another approach I tried 
N = 5
M = 10

triplets_limited = triplets_not_mismatched

popular_songs = (
    get_song_counts(triplets_limited)
    .where(col('user_count')>N)
    .cache()
    )
popular_songs.count() #263606
popular_songs.count() / song_counts.count() #  0.6967989215193888
popular_songs.show()
print(popular_songs.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
# [6.0, 18.0, 30.0, 103.0, 90444.0]

active_users = (
    get_user_counts(triplets_limited)
    .where(col('song_count') > M)
    .cache()
    )
active_users.count() #939475
active_users.count() / user_counts.count() # 0.9216701755487493
active_users.show()
print(active_users.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
# [11.0, 18.0, 33.0, 69.0, 4316.0]

triplets_limited = (
    triplets_limited
    .join(popular_songs.select("song_id"), on='song_id', how='inner')
    .join(active_users.select("user_id"), on='user_id', how='inner')
    )
triplets_limited.cache()
triplets_limited.count() # 44733008
print(get_user_counts(triplets_limited).approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
print(get_song_counts(triplets_limited).approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
# [1.0, 19.0, 31.0, 72.0, 4072.0]
# [4.0, 15.0, 31.0, 109.0, 86640.0]

# More iterations will be need

triplets_limited.show()
triplets_limited.count() / triplets_not_mismatched.count() 


# -----------------------------------------------------------------------------
# -----------------------------Q1 (e)------------------------------------------
# -----------------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.feature import StringIndexer

# Encoding

user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_encoded")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="song_id_encoded")

user_id_indexer_model = user_id_indexer.fit(triplets_limited)
song_id_indexer_model = song_id_indexer.fit(triplets_limited)

triplets_limited = user_id_indexer_model.transform(triplets_limited)
triplets_limited = song_id_indexer_model.transform(triplets_limited)

# -----------------------------------------------------------------------------
# Splitting
# -----------------------------------------------------------------------------

# Imports

from pyspark.sql.window import *

# Splits

training, test = triplets_limited.randomSplit([0.7, 0.3])
test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

# training:          31290034
# test:              13407373
# test_not_training: 0

training.dropDuplicates(['user_id']).count() #936546
test.dropDuplicates(['user_id']).count() #266306

# --------------------------------------------------------------
# ---------------------------Q2-------------------------------
# -----------------------------------------------------------------------------
# (a) Modeling
# -----------------------------------------------------------------------------
# Imports
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Modeling
als = ALS(maxIter=5, regParam=0.01, userCol="user_id_encoded", itemCol="song_id_encoded", ratingCol="plays", implicitPrefs=True)
als_model = als.fit(training)

predictions = als_model.transform(test)
predictions = predictions.orderBy(col("user_id"), col("song_id"), col("prediction").desc())
predictions.cache()

predictions.show(50, False)

#+------------------+----------------------------------------+-----+---------------+---------------+------------+
#|song_id           |user_id                                 |plays|user_id_encoded|song_id_encoded|prediction  |
#+------------------+----------------------------------------+-----+---------------+---------------+------------+
#|SOCZQCY12AC468E40F|00000b722001882066dff9d2da8a775658053ea0|1    |861037.0       |12948.0        |0.7013655   |
#|SOFLJQZ12A6D4FADA6|00000b722001882066dff9d2da8a775658053ea0|1    |861037.0       |6.0            |0.9355942   |
#|SOJOJUN12A8AE47E1D|00000b722001882066dff9d2da8a775658053ea0|1    |861037.0       |1164.0         |1.6976873   |
#|SOAORYL12A67AD8187|00001638d6189236866af9bbf309ae6c2347ffdc|7    |787952.0       |189383.0       |4.851613    |
#|SOBFEDK12A8C13BB25|00001638d6189236866af9bbf309ae6c2347ffdc|1    |787952.0       |17800.0        |-11.019085  |
#|SOLOYFG12A8C133391|00001638d6189236866af9bbf309ae6c2347ffdc|1    |787952.0       |19780.0        |1.3240907   |
#|SONGKIR12A58A779D3|00001638d6189236866af9bbf309ae6c2347ffdc|5    |787952.0       |117266.0       |8.459994    |
#|SOACWNM12AB0182F2D|0000175652312d12576d9e6b84f600caa24c4715|1    |862319.0       |22750.0        |-1.1971569  |
#|SOBRHVR12A8C133F35|0000175652312d12576d9e6b84f600caa24c4715|1    |862319.0       |142.0          |-2.544794   |
#|SOBYRTY12AB0181EDB|0000175652312d12576d9e6b84f600caa24c4715|1    |862319.0       |7819.0         |0.6795049   |
#|SONQERE12A8C136364|0000175652312d12576d9e6b84f600caa24c4715|1    |862319.0       |36322.0        |0.104916394 |
#|SOPZAEV12A6D4FAD60|0000175652312d12576d9e6b84f600caa24c4715|4    |862319.0       |76894.0        |-0.595706   |
#|SOJAMXH12A8C138D9B|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688084.0       |1846.0         |0.9093989   |
#|SOLHJTO12A81C21354|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|1    |688084.0       |28155.0        |0.5277994   |
#|SOLOVPR12AB0182D03|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688084.0       |3322.0         |1.8897038   |
#|SOMMIUM12A8C13FD49|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688084.0       |4198.0         |0.77682936  |
#|SOZFJYG12AB0182D24|00001cf0dce3fb22b0df0f3a1d9cd21e38385372|2    |688084.0       |4174.0         |1.0049585   |
#|SOBMSCQ12AAF3B51B7|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |1722.0         |0.33727998  |
#|SOCJCVE12A8C13CDDB|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |411.0          |1.071944    |
#|SOHOTAA12A8AE45F43|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |259.0          |1.0222974   |
#|SOIYHAZ12A6D4F6D30|0000267bde1b3a70ea75cf2b2d216cb828e3202b|2    |553368.0       |548.0          |0.9154923   |
#|SOMCHHT12A8C13B9F2|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |11688.0        |0.800128    |
#|SOPQGWI12A8C135DDB|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |3049.0         |0.94143116  |
#|SOTDKEJ12AB0187AAA|0000267bde1b3a70ea75cf2b2d216cb828e3202b|2    |553368.0       |432.0          |1.1050947   |
#|SOYHTAT12A81C23955|0000267bde1b3a70ea75cf2b2d216cb828e3202b|1    |553368.0       |1901.0         |1.098292    |
#|SOEOODW12AB017A9FB|00003a4459f33b92906be11abe0e93efc423c0ff|1    |788054.0       |127408.0       |12.070724   |
#|SOGFFET12A58A7ECA9|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |222.0          |1.531704    |
#|SOGXWGC12AF72A8F9A|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |2016.0         |0.7663268   |
#|SOGZCOB12A8C14280E|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|2    |325379.0       |463.0          |1.7785733   |
#|SOPDRWC12A8C141DDE|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |338.0          |1.49525     |
#|SOPKPUK12A8C13CAED|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|3    |325379.0       |1649.0         |1.4797168   |
#|SOPWKOX12A8C139D43|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|4    |325379.0       |2039.0         |1.1917467   |
#|SOSNTSY12AF72A7B43|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |670.0          |1.2904747   |
#|SOWRMTT12A8C137064|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |952.0          |1.8358023   |
#|SOXLKNJ12A58A7E09A|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|3    |325379.0       |808.0          |1.4309886   |
#|SOZVCRW12A67ADA0B7|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|1    |325379.0       |37.0           |1.2643039   |
#|SOHCOVJ12A8C137A8E|00005c6177188f12fb5e2e82cdbd93e8a3f35e64|1    |774064.0       |41305.0        |3.740318    |
#|SOVATGC12A6D4F9393|00005c6177188f12fb5e2e82cdbd93e8a3f35e64|1    |774064.0       |11172.0        |-3.8366685  |
#|SOAAVNU12A6D4F65BA|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |11322.0        |2.615978    |
#|SOAPSFK12AC46890F8|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |9159.0         |0.06027615  |
#|SOBNOGD12AC90971C5|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |33409.0        |-2.27646    |
#|SOCUYKP12A8C13CDD0|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|5    |416081.0       |5364.0         |6.3810124   |
#|SOGZHPN12AB0181074|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|6    |416081.0       |47914.0        |9.200327    |
#|SOHLBZD12A58A7B0AC|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |5258.0         |-0.26848486 |
#|SOIEPEN12A6D4F86F2|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |6873.0         |0.9867368   |
#|SOILWTV12A6D4F4A4B|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |6277.0         |-0.052952737|
#|SONSHOY12A8C13E81D|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |82007.0        |1.3374159   |
#|SORTDVT12A8AE46092|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|5    |416081.0       |2536.0         |-3.1629262  |
#|SORWXBN12A6701E3E7|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |34892.0        |0.6197721   |
#|SOVSLCI12A8C137ED3|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|1    |416081.0       |9025.0         |0.99492556  |
#+------------------+----------------------------------------+-----+---------------+---------------+------------+


# Evaluate the model by computing the RMSE on the test data

import numpy as np
evaluator = RegressionEvaluator(metricName="rmse", labelCol="plays", predictionCol="prediction")
rmse = evaluator.evaluate(predictions.filter(col('prediction') != np.NaN))
print(rmse) #12.658483360069006

# --------------------------------------------------------------
# (b) Comparison between recommendation and acutally played 
# --------------------------------------------------------------

# Helpers 

def extract_songs_top_k(x, k):
    x = sorted(x, key=lambda x: -x[1])
    return [x[0] for x in x][0:k]

extract_songs_top_k_udf = udf(lambda x: extract_songs_top_k(x, k), ArrayType(IntegerType()))

def extract_songs(x):
    x = sorted(x, key=lambda x: -x[1])
    return [x[0] for x in x]

extract_songs_udf = udf(lambda x: extract_songs(x), ArrayType(IntegerType()))

# Recommendations (top10)

k = 10
topK = als_model.recommendForAllUsers(k)

topK.cache()
topK.count() # 936546
topK.show(10, False)
#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|user_id_encoded|recommendations                                                                                                                                                                                                 |
#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|12             |[[148931, 508.41675], [91928, 405.86917], [240258, 400.72937], [224776, 350.91595], [178026, 332.37222], [218996, 318.96893], [39565, 315.20227], [198191, 295.72736], [129877, 293.70697], [139267, 292.08167]]|
#|18             |[[212446, 220.41599], [147682, 179.98102], [221508, 144.2173], [181004, 128.3598], [129992, 121.06968], [96882, 119.29312], [191252, 117.24858], [68659, 114.09888], [116237, 102.99777], [157113, 94.81799]]   |
#|38             |[[157113, 258.9511], [147682, 188.5995], [91928, 152.2443], [116237, 124.16924], [125665, 115.14264], [221508, 113.208374], [98580, 109.27501], [169738, 99.10109], [104986, 96.77531], [68183, 93.052025]]     |
#|70             |[[212446, 235.33783], [157113, 223.39908], [71728, 121.652405], [70709, 108.03835], [130553, 105.69083], [191252, 98.75618], [116237, 90.334595], [120789, 83.21695], [90513, 76.6774], [108515, 73.27618]]     |
#|93             |[[212446, 308.7847], [156333, 223.3357], [39565, 167.61385], [107169, 165.25311], [116237, 163.31998], [127829, 161.19374], [147682, 146.20717], [129877, 130.37204], [157113, 127.79642], [130553, 122.07265]] |
#|190            |[[147682, 150.37808], [212446, 124.26132], [116237, 116.754364], [96882, 101.87916], [221508, 99.74751], [240258, 97.59148], [129992, 90.78669], [60823, 85.12467], [180534, 76.29307], [32513, 75.621735]]     |
#|218            |[[212446, 167.36407], [147682, 165.76276], [116237, 113.66669], [91928, 112.10994], [240258, 104.95984], [129992, 86.885574], [129877, 85.6075], [96882, 85.06804], [60823, 84.59555], [32513, 76.75193]]       |
#|273            |[[129877, 198.3363], [212446, 195.71045], [47544, 168.23346], [211101, 152.21466], [66126, 125.4211], [134832, 122.34018], [191252, 112.78772], [39565, 111.64654], [156333, 107.372604], [246017, 104.3893]]   |
#|300            |[[147682, 51.067863], [211101, 45.42917], [91928, 35.87502], [221508, 34.66795], [240258, 30.18777], [116237, 29.067173], [212446, 28.464287], [129992, 27.875942], [32513, 26.122349], [218996, 25.755667]]    |
#|340            |[[157113, 124.696495], [212446, 118.507904], [147682, 116.98317], [116237, 84.01076], [96882, 77.5659], [221508, 75.50624], [129992, 64.94632], [98580, 56.040016], [191252, 52.555946], [106064, 50.3929]]     |
#+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

recommended_songs = (
    topK
    .withColumn("recommended_songs", extract_songs_udf(col("recommendations")))
    .select("user_id_encoded", "recommended_songs")
)

recommended_songs.cache()
recommended_songs.count() # 936546
recommended_songs.show(10, False)

#+---------------+-------------------------------------------------------------------------------+
#|user_id_encoded|recommended_songs                                                              |
#+---------------+-------------------------------------------------------------------------------+
#|12             |[148931, 91928, 240258, 224776, 178026, 218996, 39565, 198191, 129877, 139267] |
#|18             |[212446, 147682, 221508, 181004, 129992, 96882, 191252, 68659, 116237, 157113] |
#|38             |[157113, 147682, 91928, 116237, 125665, 221508, 98580, 169738, 104986, 68183]  |
#|70             |[212446, 157113, 71728, 70709, 130553, 191252, 116237, 120789, 90513, 108515]  |
#|93             |[212446, 156333, 39565, 107169, 116237, 127829, 147682, 129877, 157113, 130553]|
#|190            |[147682, 212446, 116237, 96882, 221508, 240258, 129992, 60823, 180534, 32513]  |
#|218            |[212446, 147682, 116237, 91928, 240258, 129992, 129877, 96882, 60823, 32513]   |
#|273            |[129877, 212446, 47544, 211101, 66126, 134832, 191252, 39565, 156333, 246017]  |
#|300            |[147682, 211101, 91928, 221508, 240258, 116237, 212446, 129992, 32513, 218996] |
#|340            |[157113, 212446, 147682, 116237, 96882, 221508, 129992, 98580, 191252, 106064] |
#+---------------+-------------------------------------------------------------------------------+


# Relevant songs

relevant_songs = (
    test
    .select(
        col("user_id_encoded").cast(IntegerType()),
        col("song_id_encoded").cast(IntegerType()),
        col("plays").cast(IntegerType())
    )
    .groupBy('user_id_encoded')
    .agg(
        collect_list(
            array(
                col("song_id_encoded"),
                col("plays")
            )
        ).alias('relevance')
    )
    .withColumn("relevant_songs", extract_songs_top_k_udf(col("relevance")))
    .select("user_id_encoded", "relevant_songs")
)
relevant_songs.cache()
relevant_songs.count() # 934046

relevant_songs.show(10, False)

#+---------------+-----------------------------------------------------------------------+
#|user_id_encoded|relevant_songs                                                         |
#+---------------+-----------------------------------------------------------------------+
#|12             |[9147, 50346, 48364, 1786, 9884, 18533, 44524, 48841, 19610, 63915]    |
#|18             |[0, 36829, 32593, 33343, 31059, 1107, 36041, 37, 188, 2150]            |
#|38             |[17149, 11270, 284, 32080, 7028, 33662, 6815, 57982, 51646, 3309]      |
#|70             |[418, 8445, 152938, 1664, 4982, 6265, 5518, 31089, 94488, 1771]        |
#|93             |[1461, 119194, 131935, 43393, 9641, 4186, 7881, 18644, 26195, 1085]    |
#|190            |[8456, 15920, 8533, 17727, 5046, 1221, 232, 322, 1162, 1808]           |
#|218            |[284, 33150, 2933, 3976, 5233, 8074, 2333, 478, 5469, 434]             |
#|273            |[409, 15993, 236, 1629, 1979, 17297, 3446, 12616, 41106, 2976]         |
#|300            |[123621, 17230, 3105, 12533, 245267, 8480, 85569, 46004, 245705, 37385]|
#|340            |[59604, 9916, 7386, 31820, 10774, 6858, 4570, 10847, 3122, 31101]      |
#+---------------+-----------------------------------------------------------------------+


combined = (
    recommended_songs.join(relevant_songs, on='user_id_encoded', how='inner')
    .rdd
    .map(lambda row: (row[1], row[2]))
)
combined.cache()
combined.count() # 934046

combined.take(5)
#([148931,91928,240258,224776,178026, 218996, 39565, 198191, 129877, 139267],[9147, 50346, 48364, 1786, 9884, 18533, 44524, 48841, 19610, 63915])
#([212446, 147682, 221508, 181004, 129992, 96882, 191252, 68659, 116237, 157113],[0, 36829, 32593, 33343, 31059, 1107, 36041, 37, 188, 2150])
#([157113, 147682, 91928, 116237, 125665, 221508, 98580, 169738, 104986, 68183],[17149, 11270, 284, 32080, 7028, 33662, 6815, 57982, 51646, 3309])
#([212446, 157113, 71728, 70709, 130553, 191252, 116237, 120789, 90513, 108515],[418, 8445, 152938, 1664, 4982, 6265, 5518, 31089, 94488, 1771])
#([212446, 156333, 39565, 107169, 116237, 127829, 147682, 129877, 157113, 130553],[1461, 119194, 131935, 43393, 9641, 4186, 7881, 18644, 26195, 1085])


# -----------------------------------------------------------------------------
#  (c) Evaluation Metrics
# -----------------------------------------------------------------------------
from pyspark.mllib.evaluation import RankingMetrics

rankingMetrics = RankingMetrics(combined)

precisionAtK = rankingMetrics.precisionAt(5)
print("Precision @ 5: ", precisionAtK) # 8.350766450474605e-06

ndcgAtK = rankingMetrics.ndcgAt(10)
print("NDCG @ 10: ", ndcgAtK) # 1.0431732372769594e-05

MAP = rankingMetrics.meanAveragePrecision
print("Mean Average Precision (MAP): ", MAP ) #  3.2688133151511535e-06








