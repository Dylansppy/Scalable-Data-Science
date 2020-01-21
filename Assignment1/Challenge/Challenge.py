###### DATA420 Assignment 1 Peng Shen (student ID: 57408055)
##### Challenge

start_pyspark_shell -e 4 -c 2 -w 4 -m 4

# Python and pyspark modules required
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 
import pandas as pd

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf

N = int(conf.get("spark.executor.instances")
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M


### Q1 
# Investigate the coverage of the other element temporally and geographically
# First I'd like to investigate the frequecy of each of other elements collected by stations over the world temporally
# Laod the whole daily
schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", IntegerType(), True), 
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True) 
])

daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/")
)
daily_all.show(5, False)

# Extract YEAR from DATE
daily_all = (
    daily_all
    .withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))
)
daily_all.show(5, False)

# Get a subset of daily with other elements
core_element = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']
daily_other = (
    daily_all
    .filter(~F.col('ELEMENT').isin(core_element))
)
daily_other.show(5, False)


# check the count of daily obersevation by element
daily_by_element = (
    daily_other
    .groupby('ELEMENT')
    .count()
    .sort('count', ascending=False)
)
daily_by_element.cache()
daily_by_element.show()

# export the dataset as csv for further analysis
element_frequency = daily_by_element.toPandas()
element_frequency.to_csv('element_frequency.csv')

# filter all the observation belong to top_20 element
frequent_elements = (
    daily_by_element
    .rdd
    .map(lambda x: x.ELEMENT)
    .collect()
)
top_20 = frequent_elements[0:10]
daily_other_20 = daily_other.filter(F.col('ELEMENT').isin(top_20))

# Create a table ragarding the count of stations collecting each 'other elements' each day
element_count_by_day = (
    daily_other_20
    .groupby(F.col('ELEMENT'), F.col('DATE'))
    .agg({'ID':'count'})
    .select('ELEMENT', 'DATE', F.col('count(ID)').alias('ELEMENT_COUNT'))
)
element_count_by_day.cache()
element_count_by_day.show(5, False)
# Save the table to home directory
element_count_by_day.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/element_count_by_day.parquet")
# Copy to local for plotting
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/element_count_by_day.parquet ~/element_count_by_day.parquet



# Then I decide to investigate the average daily wind speed (AWND) for each state in the US for year 2015
stations_US= (
    spark.read.parquet("hdfs:///user/psh99/outputs/ghcnd/stations.parquet")
    .filter(F.col('COUNTRY_CODE')=='US')
)
stations_US.cache()
stations_US.show(5)

daily_AWND_country_2015 = (
    daily_other
    .filter((F.col('YEAR') == '2015') & (F.col('ELEMENT')=='AWND'))
    .join(
        F.broadcast(stations_US.select("ID", "STATE", "STATE_NAME", "LATITUDE", "LONGITUDE")),
        on="ID",
        how="inner"
    )
)
daily_AWND_country_2015.cache()
daily_AWND_country_2015.show(5, False)
# Save the table to home directory
daily_AWND_country_2015.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/daily_AWND_country_2015.parquet")
# Copy to local for plotting
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/daily_AWND_country_2015.parquet ~/daily_AWND_country_2015.parquet

# Check the quality of the daily_AWND_country_2015
daily_AWND_country_2015.count() #362751 observations in total
daily_AWND_country_2015.groupby('QUALITY_FLAG').count().show()

# Final table for time series of daily average wind speed for each states in 2015
daily_AWND_by_states = (
    daily_AWND_country_2015
    .groupby('DATE', 'STATE')
    .agg(F.mean('VALUE').alias('AVG_STATE_WIND'))
)
daily_AWND_by_states.cache()
daily_AWND_by_states.show(5, False)
# Save the table to home directory
daily_AWND_by_states.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/daily_AWND_by_states.parquet")
# Copy to local for plotting
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/daily_AWND_by_states.parquet ~/daily_AWND_by_states.parquet
    
# Final table for geographical map of average wind speed of each states in 2015
AWND_2015_by_states = (
    daily_AWND_country_2015
    .groupby('STATE', 'STATE_NAME')
    .agg(F.mean('VALUE').alias('AWND_2015'))
)
AWND_2015_by_states.cache()
AWND_2015_by_states.show(5, False)
# Save the table to home directory
AWND_2015_by_states.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/AWND_2015_by_states.parquet")
# Copy to local for plotting
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/AWND_2015_by_states.parquet ~/AWND_2015_by_states.parquet
    

#Q2
# Count the number of observations with each of quality flags
daily_all.count() #2624027105 observations in total
daily_all.groupby('QUALITY_FLAG').count().sort('count', ascending = False).show()


