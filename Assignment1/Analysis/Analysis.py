###### DATA420 Assignment 1 Peng Shen (student ID: 57408055)
##### Analysis

start_pyspark_shell -e 4 -c 2 -w 4 -m 4

# Python and pyspark modules required

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M


### Q1 (a)
# How many stations are there in total?
stations.count()  #103656

# How many stations have been active in 2017?
stations.filter((F.col('FIRSTYEAR_ACTIVE') <=2017)&(F.col('LASTYEAR_ACTIVE')>=2017)).count() #37546

# How many stations are in the GCOS Surface Network(GSN)?
stations.filter(F.col('GSN_FLAG')=='GSN').count() #991

# How many stations are in the US Historical Climatology Network(HCN)?
stations.filter(F.col('HCN/CRN_FLAG')=='HCN').count() #1218

# How many stations are in the US Climate Reference Network(CRN)?
stations.filter(F.col('HCN/CRN_FLAG')=='CRN').count() #230

# Are there any stations that are in more than one of these networks? 
stations.filter((F.col('GSN_FLAG')!='') & (F.col('HCN/CRN_FLAG')!='')).count() #14


### Q1 (b)
# Count the total number of stations in each country
station__count_by_country = stations.groupby('COUNTRY_CODE').count()
station__count_by_country.show()
# Store the output in countries using the withColumnRenamed command
countries = (
    countries
    .join(
        station__count_by_country,
        on = countries.CODE == station__count_by_country.COUNTRY_CODE,
        how ='left'
    )
    .withColumnRenamed('count', 'STATION_COUNT')
    .drop('COUNTRY_CODE')
)
countries.show(10, False)
# Save the countries table
countries.write.format('parquet').mode('overwrite').save(output_path + 'countries.parquet')
!hdfs dfs -du -h /user/psh99/outputs/ghcnd/

# Count the total number of stations in each state, and store the output in countries using the withColumnRenamed command.
station__count_by_state = stations.groupby('STATE').count()
station__count_by_state.show(10, False)
# Store the output in states using the withColumnRenamed command
states = (
    states
    .join(
        station__count_by_state,
        on = states.CODE == station__count_by_state.STATE,
        how ='left'
    )
    .withColumnRenamed('count', 'STATION_COUNT')
    .drop('STATE')
)
states.show(10, False)
# Save the countries table
states.write.format('parquet').mode('overwrite').save(output_path + 'states.parquet')
!hdfs dfs -du -h /user/psh99/outputs/ghcnd/

### Q1 (c)
# How many stations are there in the Southern Hemisphere only?
stations.filter(F.col('LATITUDE')<0).count() # 25337

# How many stations belong to the US
stations.filter(F.col('COUNTRY_NAME').like('%United States%')).count() #57227

### Q2 (a)
# function to calculate distance from latitude and longitude
from math import sin, cos, sqrt, atan2, radians

def distance(lat1, lon1, lat2, lon2):
	"""Arg: lat/lon for 2 poins in decimal degree
	   Output: diatance in km
	"""
	r = 6373.0
	lat1 = radians(lat1)
	lon1 = radians(lon1)
	lat2 = radians(lat2)
	lon2 = radians(lon2)
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = (sin(dlat/2))**2 + cos(lat1) * cos(lat2) * (sin(dlon/2))**2
	c = 2 * atan2(sqrt(a), sqrt(1-a))
	return r * c

distance_udf = F.udf(distance, DoubleType())

#test the udf
stations_1 = (
    stations
    .limit(10)
    .select('ID', 'LATITUDE', 'LONGITUDE')
    .withColumnRenamed('ID', 'ID_1')
    .withColumnRenamed('LATITUDE', 'LATITUDE_1')
    .withColumnRenamed('LONGITUDE', 'LONGITUDE_1')
)

stations_2 = (
    stations
    .limit(10)
    .select('ID', 'LATITUDE', 'LONGITUDE')
    .withColumnRenamed('ID', 'ID_2')
    .withColumnRenamed('LATITUDE', 'LATITUDE_2')
    .withColumnRenamed('LONGITUDE', 'LONGITUDE_2')
)  

stations_pair = (
    stations_1
    .crossJoin(stations_2)
    .filter(F.col('ID_1')!=F.col('ID_2'))
)

stations_with_distance = (
    stations_pair
    .withColumn('DISTANCE', distance_udf('LATITUDE_1', 'LONGITUDE_1', 'LATITUDE_2', 'LONGITUDE_2'))
    .dropDuplicates(['DISTANCE'])
)
stations_with_distance.show(5)

### Q2 (b)
# Create a table with stations in New Zealand in pairwise
stations_NZ = stations.filter(stations.COUNTRY_CODE=='NZ').select(['ID', 'LATITUDE', 'LONGITUDE'])
stations_NZ_1 = (
	stations_NZ
	.withColumnRenamed('ID', 'ID_1')
	.withColumnRenamed('LATITUDE', 'LATITUDE_1')
	.withColumnRenamed('LONGITUDE', 'LONGITUDE_1')
)

stations_NZ_2 = (
	stations_NZ
	.withColumnRenamed('ID', 'ID_2')
	.withColumnRenamed('LATITUDE', 'LATITUDE_2')
	.withColumnRenamed('LONGITUDE', 'LONGITUDE_2')
)

stations_NZ_pairs = (
	stations_NZ_1
	.crossJoin(stations_NZ_2)
	.filter(F.col('ID_1') != F.col('ID_2'))
)

# Caculate the distance and add a new column to the table
stations_NZ_distance = (
	stations_NZ_pairs
	.withColumn('DISTANCE', distance_udf('LATITUDE_1', 'LONGITUDE_1', 'LATITUDE_2', 'LONGITUDE_2'))
    .dropDuplicates(['DISTANCE'])
)
stations_NZ_distance.show(5)

# What two stations are the geographically closest in New Zealand?
stations_NZ_distance.sort('DISTANCE').show(5) #NZ000093417 and NZM00093439 are the closest

# Save the table
stations_NZ_distance.write.save(output_path+"stations_NZ_distance.parquet", format='parquet', mode='overwrite')
!hdfs dfs -du -h hdfs:///user/psh99/outputs/ghcnd/


### Q3 (b)
# Load the daily form 2010 
daily_2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)
# Count rows
daily_2010.count() #36946080

# Load the daily form 2017
daily_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)
# Count rows
daily_2017.count() #21904999

### Q3(c)
# Load the daily form 2010 to 2015
daily_2010_2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201[0-5].csv.gz")
)
# Count rows
daily_2010_2015.count() #207716098


### Q4 (a)
# Count the number of rows in daily
daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/")
)
# Count rows
daily_all.count() # 2624027105

### Q4 (b)
# Obtain the subset of observations containing the five core elements described in inventory.
daily_all.show(10, False)
core_element = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']
daily_core = (
    daily_all
    .filter(F.col('ELEMENT').isin(core_element))
)
daily_core.show(10, False)

# How many observations are there for each of the five core elements?
daily_core_count = (
    daily_core
    .groupby('ELEMENT')
    .count()
)
daily_core_count.show(10, False)

# Which element has the most observations? Is this element consistent with Processing Q3?
daily_core_count.sort('count').show() # Precipitation has the highest count of 918490401


### Q4 (c) 
# Determine how many observations of TMIN do not have a corresponding observation of TMAX
# Create a new table contains 'ID', 'DATE' and a set of 'TMAX' and 'TMIN'
daily_T = (
    daily_core
    .filter(F.col('ELEMENT').isin(['TMAX','TMIN']))
    .groupby(['ID', 'DATE'])
    .agg(
        F.collect_set('ELEMENT').alias('T_SET')
	)
)
daily_T.show()
# Filter observations that match our condition
daily_min_only = (
    daily_T
    .filter((F.array_contains('T_SET', 'TMIN')) & (~ F.array_contains('T_SET', 'TMAX')))
)
daily_min_only.show()
daily_min_only.count() #7528188

# How many different stations contributed to these observations?
stations_min_only = daily_min_only.dropDuplicates(['ID'])
stations_min_only.count() #26625

# Do any belong to the GSN, HCN, or CRN?
(stations_min_only
    .join(
        F.broadcast(stations.select('ID', 'GSN_FLAG','HCN/CRN_FLAG')), 
        on='ID',
        how='left'
    )
    .filter(
        (F.col('GSN_FLAG')!="") | (F.col('HCN/CRN_FLAG')!="")
    )
).count() #2111

### Q4 (d)
# Load precessed stations from home directory and filter all the stations belong to NZ
stations_NZ= (
    spark.read.parquet("hdfs:///user/psh99/outputs/ghcnd/stations.parquet")
    .filter(F.col('COUNTRY_CODE')=='NZ')
)
stations_NZ.cache()
stations_NZ.show(5)

# Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand
daily_temp_NZ = (
    daily_all
    .filter(F.col('ELEMENT').isin(['TMAX','TMIN']))
	.join(
		stations_NZ,
		on='ID',
		how='inner'
		)
)
daily_temp_NZ.show()

# How many observations are there?
daily_temp_NZ.count() # 447017
daily_temp_NZ.cache()

# How many years are covered by the observations?
# Extract YEAR from DATE
daily_temp_NZ = daily_temp_NZ.withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))
# Count years
daily_temp_NZ.dropDuplicates(['YEAR']).count() #78 
# Find the start and end years
daily_temp_NZ.groupby('COUNTRY_NAME').agg(F.min('YEAR'), F.max('YEAR')).show() #1940-2017

# Save to parquer file
daily_temp_NZ.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/daily_temp_NZ.parquet")
# Check output
!hdfs dfs -du -h hdfs:///user/psh99/outputs/ghcnd/
# Copy to local
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/daily_temp_NZ.parquet ~/daily_temp_NZ.parquet

### Q4 (e)
# Extract YEAR from DATE
daily_all = daily_all.withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))

# Load precessed stations from home directory
stations= spark.read.parquet("hdfs:///user/psh99/outputs/ghcnd/stations.parquet")
stations_NZ.cache()

# Group the precipitation observations by year and country. Compute the average rainfall in each year for each country
rainfall_by_year_country = (
	daily_all
    .filter(F.col('ELEMENT')=='PRCP')
	.join(stations, on='ID', how= 'left')
	.groupby(F.col('YEAR'), F.col('COUNTRY_CODE'), F.col('COUNTRY_NAME'))
    .agg(F.mean('VALUE').alias('AVG_RAINFALL'))
	.sort(F.col('AVG_RAINFALL'), ascending=False)
)
rainfall_by_year_country.show() # Equatorial Guinea has the highest rainfall in 2000 which is 4361.0
rainfall_by_year_country.cache()

# Save the table to home directory
rainfall_by_year_country.write.format('parquet').mode('overwrite').save("hdfs:///user/psh99/outputs/ghcnd/rainfall_by_year_country.parquet")
# Check output
!hdfs dfs -du -h hdfs:///user/psh99/outputs/ghcnd/
# Copy to local
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/rainfall_by_year_country.parquet ~/rainfall_by_year_country.parquet

# Plotting preparation
cum_rainfall_by_country = (
	rainfall_by_year_country
	.groupby(F.col('COUNTRY_CODE'), F.col('COUNTRY_NAME'))
	.agg(F.mean('AVG_RAINFALL').alias('CUM_RAINFALL'))
    .sort('CUM_RAINFALL', ascending=False)
	)
cum_rainfall_by_country.show(5)
cum_rainfall_by_country.cache()
# Save the table to home directory
cum_rainfall_by_country.write.format('parquet').save("hdfs:///user/psh99/outputs/ghcnd/cum_rainfall_by_country.parquet")
# Copy to local
!hdfs dfs -copyToLocal hdfs:///user/psh99/outputs/ghcnd/cum_rainfall_by_country.parquet ~/cum_rainfall_by_country.parquet