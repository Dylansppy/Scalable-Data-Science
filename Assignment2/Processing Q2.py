###### DATA420 Assignment 2 Peng Shen (student ID: 57408055)
##### Processing

# Python and pyspark modules required
start_pyspark_shell -e 2 -c 1 -w 1 -m 1

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
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

# ----------------------------------------------------------
# -------------------Processing Q2--------------------------
# ----------------------------------------------------------
# (a)
mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

# mismatches
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

print(mismatches.count())

# matches_manually_accepted
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
matches_manually_accepted.count() # 488

# triplets
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

print(triplets.count()) # 48373586
print(triplets_not_mismatched.count()) #45795111

# Processing Q2 (b)
!hdfs dfs -cat "/data/msd/audio/attributes/*" | awk -F',' '{print $2}' | sort | uniq

# NUMERIC
# real
# real
# string
# string
# STRING

audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

# Modification Version
dataset_names = [
    "msd-jmir-area-of-moments-all-v1.0",
    "msd-jmir-lpc-all-v1.0",
    "msd-jmir-methods-of-moments-all-v1.0",
    "msd-jmir-mfcc-all-v1.0",
    "msd-jmir-spectral-all-all-v1.0",
    "msd-jmir-spectral-derivatives-all-all-v1.0",
    "msd-marsyas-timbral-v1.0",
    "msd-mvd-v1.0",
    "msd-rh-v1.0",
    "msd-rp-v1.0",
    "msd-ssd-v1.0",
    "msd-trh-v1.0",
    "msd-tssd-v1.0"
    ]
    
def load_attributes_file_and_convert_to_schema(attributes_path, type_mapping=audio_attribute_type_mapping):
    f = open(attributes_path)
    rows = [line.strip().split(",") for line in f.readlines()]
    return StructType([StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows])
    
def load_dataset(dataset_name):
    attributes_path = f"/scratch-network/courses/2019/DATA420-19S2/data/msd/audio/attributes/{dataset_name}.attributes.csv"
    features_path = f"hdfs:///data/msd/audio/features/{dataset_name}.csv"
    dataset_schema = load_attributes_file_and_convert_to_schema(attributes_path)
    df = (
        spark.read
        .format("csv")
        .option("header", "false")
        .schema(dataset_schema)
        .load(features_path)
    )
    return df
    
datasets = {dataset_name: load_dataset(dataset_name) for dataset_name in dataset_names}

# Check if the functions work
msd_jmir_area_of_moments = load_dataset("msd-jmir-area-of-moments-all-v1.0")
msd_jmir_area_of_moments.show(10)
msd_jmir_area_of_moments = datasets["msd-jmir-area-of-moments-all-v1.0"]
msd_jmir_area_of_moments.show(10)

    
