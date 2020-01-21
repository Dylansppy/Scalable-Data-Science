###### DATA420 Assignment 1 Peng Shen (student ID: 57408055)
##### Processing
####  Q2
start_pyspark_shell -e 2 -c 1 -w 1 -m 1

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

### Q2(a)

# Define schema of daily
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

# Define schema of stations
schema_stations = StructType([
                           StructField("ID", StringType(), True),
                           StructField("LATITUDE", DoubleType(), True), 
                           StructField("LONGITUDE", DoubleType(), True), 
                           StructField("ELEVATION", DoubleType(), True), 
                           StructField("STATE", StringType(), True),
                           StructField("STATION_NAME", StringType(), True),
                           StructField("GSN_FLAG", StringType(), True),
                           StructField("HCN/CRN_FLAG", StringType(), True),
                           StructField("WMO_ID", StringType(), True)
                           ])

# Define schema of states
schema_states = StructType([
                           StructField("CODE", StringType(), True),
                           StructField("NAME", StringType(), True),
                           ])

# Define schema of countries
schema_countries = StructType([
                           StructField("CODE", StringType(), True),
                           StructField("NAME", StringType(), True),
                           ])

# Define schema of inventory
schema_inventory = StructType([
                           StructField("ID", StringType(), True),
                           StructField("LATITUDE", DoubleType(), True), 
                           StructField("LONGITUDE", DoubleType(), True), 
                           StructField("ELEMENT", StringType(), True), 
                           StructField("FIRSTYEAR", IntegerType(), True),
                           StructField("LASTYEAR", IntegerType(), True),
                           ])

### Q2(b)

# Check the top of each data file to check the schema is as described
!hdfs dfs -cat /data/ghcnd/daily/2017.csv.gz | gunzip | head
!hdfs dfs -cat /data/ghcnd/stations | head
!hdfs dfs -cat /data/ghcnd/countries | head
!hdfs dfs -cat /data/ghcnd/states | head
!hdfs dfs -cat /data/ghcnd/inventory | head

## Load daily

daily = (
         spark.read.format("com.databricks.spark.csv")
         .option("header", "false")
         .option("inferSchema", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
         .limit(1000)
         )
         
# Check the table
daily.cache()
daily.show(200, False)

### Q2 (c)

## Load stations into dataframe
stations_text_only = spark.read.format('text').load("hdfs:///data/ghcnd/stations")
stations_text_only.show(10, False)

stations = stations_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_stations['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_stations['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_stations['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 6)).alias('ELEVATION').cast(schema_stations['ELEVATION'].dataType),
    F.trim(F.substring(F.col('value'), 39, 2)).alias('STATE').cast(schema_stations['STATE'].dataType),
    F.trim(F.substring(F.col('value'), 42, 30)).alias('STATION_NAME').cast(schema_stations['STATION_NAME'].dataType),
    F.trim(F.substring(F.col('value'), 73, 3)).alias('GSN_FLAG').cast(schema_stations['GSN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 77, 3)).alias('HCN/CRN_FLAG').cast(schema_stations['HCN/CRN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 81, 5)).alias('WMO_ID').cast(schema_stations['WMO_ID'].dataType)
    )
    

stations.cache()
stations.show(10, False)
              
# Count rows 103656
stations.count()

# How many stations that don't have WMO ID? 95594
stations.filter(stations["WMO_ID"]== '').count()

## Load states into dataframe
states_text_only = spark.read.format('text').load("hdfs:///data/ghcnd/states")
states_text_only.show(10, False)

states = states_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_states['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_states['NAME'].dataType)
    )  

states.cache()
states.show(10, False)
# Count rows 74
states.count()  

## Load countries into dataframe
countries_text_only = spark.read.format('text').load("hdfs:///data/ghcnd/countries")
countries_text_only.show(10, False)

countries = countries_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_countries['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_countries['NAME'].dataType)
    )  

countries.cache()
states.show(10, False)
# Count rows 218
countries.count()

## Load inventory into dataframe            
inventory_text_only = spark.read.format('text').load("hdfs:///data/ghcnd/inventory")
inventory_text_only.show(10, False)

inventory = inventory_text_only.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_inventory['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_inventory['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_inventory['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 4)).alias('ELEMENT').cast(schema_inventory['ELEMENT'].dataType),
    F.trim(F.substring(F.col('value'), 37, 4)).alias('FIRSTYEAR').cast(schema_inventory['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col('value'), 42, 4)).alias('LASTYEAR').cast(schema_inventory['LASTYEAR'].dataType)
    )  
 
inventory.cache()
inventory.show(10, False)
# Count rows 
inventory.count() #595699

#### Q3

### Q3 (a)
stations = stations.withColumn("COUNTRY_CODE", F.substring(F.col("ID"),1,2))
stations.cache()
stations.show(10, False)
# Check if there is any station that doesn't have COUNTRY CODE
stations.filter(stations["COUNTRY_CODE"]!='').count() # 103656

### Q3 (b)
stations = (
    stations
    .join(
        countries,
        on=stations["COUNTRY_CODE"]==countries.CODE,
        how="left"
    )
    .withColumnRenamed("NAME", "COUNTRY_NAME")
    .drop("CODE")
)
stations.cache()    
stations.show(10, False)

### Q3 (c)
stations = (
    stations
    .join(
        states,
        on=stations["STATE"] ==states.CODE,
        how="left"
    )
    .withColumnRenamed("NAME", "STATE_NAME")
    .drop("CODE")
)
stations.cache()    
stations.show(10, False)

### Q3 (d)
# find the first and last year that each station was active 
inventory.filter(inventory.ELEMENT.isNull()).count()

stations_by_active = (
    inventory
    .groupby('ID')
    .agg(
        F.min('FIRSTYEAR').alias('FIRSTYEAR_ACTIVE'),
        F.max('LASTYEAR').alias('LASTYEAR_ACTIVE')
    )
)
stations_by_active.cache()
stations_by_active.show(10, False)

# find how many elements has each station collected overall
stations_by_element = (
    inventory
    .dropDuplicates()
    .groupby('ID')
    .agg({'ELEMENT':'count'})
    .select('ID', F.col('count(ELEMENT)').alias('ELEMENT_COUNT'))
)
stations_by_element.cache()
stations_by_element.sort('ELEMENT_COUNT', ascending=False).show(10, False)

# count seperately the number of core elements and the number of 'other' elements that each station has collected overall
core_element = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']
stations_by_core_element = (
    inventory
    .filter(inventory.ELEMENT.isin(core_element))
    .dropDuplicates()
    .groupby('ID')
    .agg({'ELEMENT':'count'})
    .select('ID', F.col('count(ELEMENT)').alias('CORE_ELEMENT_COUNT'))
)
stations_by_core_element.sort('CORE_ELEMENT_COUNT', ascending=False).show(10, False)

stations_with_element_counts = (
    stations_by_element
    .join(
        stations_by_core_element,
        on=stations_by_element.ID==stations_by_core_element.ID,
        how="left"
    )
    .drop(stations_by_core_element.ID)
    .withColumn('OTHER_ELEMENT_COUNT', F.col('ELEMENT_COUNT')-F.col('CORE_ELEMENT_COUNT'))
)
stations_with_element_counts.cache()
stations_with_element_counts.show(10, False)

# How many stations collect all five core element?
stations_with_element_counts.filter(F.col('CORE_ELEMENT_COUNT')==5).count() #20224

# How many stations only collect precipitation?
stations_with_element_counts.filter(F.col('CORE_ELEMENT_COUNT')==1).join(inventory.filter(F.col('ELEMENT')=='PRCP'),on='ID', how='inner').count() # 29006

### Q3 (e)
stations = (
    stations
    .join(
        stations_with_element_counts,
        on='ID',
        how="left"
    )
    .join(
        stations_by_active,
        on='ID',
        how="left"
    )
)
stations.cache()
stations.show(20, False)
# Save the output
output_path = "hdfs:///user/psh99/outputs/ghcnd/"
stations.write.format('parquet').mode('overwrite').save(output_path+'stations.parquet')

!hdfs dfs -du -h /user/psh99/outputs/ghcnd/

### Q3 (f)
daily_with_stations = (
    daily
    .join(
        stations,
        on='ID',
        how='left'
    )
)
daily_with_stations.show()

# Are there any stations in the subset of daily that are not in stations at all?
daily_with_stations.filter(F.isnull('STATION_NAME')).count() #0

# If there are any stations in daily that are not in stations without using LEFT JOIN?
 daily.select('ID').dropDuplicates().subtract(stations.select('ID')).count() #0
