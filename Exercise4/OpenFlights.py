# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Load

# Define the schema of routes dataframe and datatype
schema_routes = StructType([
    StructField("airline", StringType(), True),
    StructField("airline_id", StringType(), True),
    StructField("src_airport", StringType(), True),
    StructField("src_airport_id", StringType(), True),
    StructField("dst_airport", StringType(), True),
    StructField("dst_airport_id", StringType(), True),
    StructField("codeshare", StringType(), True),
    StructField("stops", IntegerType(), True),
    StructField("equipment", StringType(), True),
])
routes = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_routes)
    .load("hdfs:///data/openflights/routes.dat")
)
routes.cache()
routes.show() 

# Define the schema of airports table and datatype
schema_airports = StructType([
  StructField("airport_id", IntegerType(), True), # True means allowing to have NULL value
  StructField("airport", StringType(), True),
  StructField("city", StringType(), True),
  StructField("country", StringType(), True),
  StructField("iata", StringType(), True),
  StructField("icao", StringType(), True),
  StructField("latitude", DoubleType(), True),
  StructField("longitude", DoubleType(), True),
  StructField("altitude", DoubleType(), True),
  StructField("timezone", DoubleType(), True),
  StructField("dst", StringType(), True),
  StructField("tz", StringType(), True),
  StructField("type", StringType(), True),
  StructField("source", StringType(), True),
])
airports = (
    spark.read.format("com.databricks.spark.csv") #Load file using SparkSession instead of SparkContext, which returns a better interface
    .option("header", "false") # Don't use default header option
    .option("inferSchema", "false") # Don't use default schema
    .schema(schema_airports) # Use shcema we've just defined
    .load("hdfs:////data/openflights/airports.dat")
)
airports.cache() # Persist my partition in memory
airports.show() 

# Spark SQL (back to front)

airports.registerTempTable('airports_tbl') # transform RDD to a table for SQL to deal with
routes.registerTempTable('routes_tbl')

counts_sql = """
    SELECT
    airports_tbl.airport AS airport_name,
    counts_tbl.size as size
    FROM
    (
        SELECT
        routes_tbl.src_airport_id AS airport_id,
        count(routes_tbl.dst_airport_id) AS size
        FROM
        routes_tbl
        GROUP BY airport_id
        ORDER BY size DESC
    ) counts_tbl
    LEFT JOIN airports_tbl ON counts_tbl.airport_id = airports_tbl.airport_id
"""
counts = spark.sql(counts_sql)
counts.show()

# Pyspark SQL API (The whole workflow can be devided into branches, ie., define and use component to make it easier to understand and develop)

from pyspark.sql import functions as F

counts = (
    routes
    # Select only the columns that are needed
    .select(['src_airport_id', 'dst_airport_id'])
    # Group by source and count destinations
    .groupBy('src_airport_id')
    .agg({'dst_airport_id': 'count'})
    .orderBy('count(dst_airport_id)', ascending=False)
    .select(
        F.col('src_airport_id').alias('airport_id'),
        F.col('count(dst_airport_id)').alias('airport_size')
    )
    # Merge with airports to get airport name
    .join(
        airports
        .select(
            F.col('airport_id'),
            F.col('airport').alias('airport_name')
        ),
        on='airport_id',
        how='left'
    )
    # Select columns to be retained
    .select(
        F.col('airport_name'),
        F.col('airport_size')
    )
)
counts.show()


# Q1 (a)Describe in your own words the data transformation that each step is doing. 
#       Is there any difference between the data transformation done by Spark SQL and the Python Spark API?
#    Ans:


# Q1 (b) Find the 10 airlines with the most routes around the world.

# Define the schema of airlines dataframe and datatype
schema_airlines = StructType([
    StructField("airline_id", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("alias", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("country", StringType(), True),
    StructField("active", StringType(), True),
])
# Create airlines
airlines = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_airlines)
    .load("hdfs:///data/openflights/airlines.dat")
)
airlines.cache()
airlines.show() 

# transform RDD to a table 
routes.registerTempTable('routes_tbl')
airlines.registerTempTable('airlines_tbl')

counts_airline_routes_sql = """
    SELECT
    airlines_tbl.airline_name AS airline_name,
    counts_tbl.size AS num_routes
    FROM
    (
        SELECT
        routes_tbl.airline_id AS airline_id,
        count(routes_tbl.dst_airport_id) AS size
        FROM
        routes_tbl
        GROUP BY airline_id
        ORDER BY size DESC
    ) counts_tbl
    LEFT JOIN airlines_tbl ON counts_tbl.airline_id = airlines_tbl.airline_id
"""
counts_airline_routes = spark.sql(counts_airline_routes_sql)
counts_airline_routes.show(10)


# Q1 (c) Find the 10 airlines which fly to the most airports around the world.
# transform RDD to a table 
routes.registerTempTable('routes_tbl')
airlines.registerTempTable('airlines_tbl')

counts_airline_dst_airports_sql = """
    SELECT
    airlines_tbl.airline_name AS airline_name,
    counts_tbl.size AS num_dst_airport
    FROM
    (
        SELECT
        routes_tbl.airline_id AS airline_id,
        count(DISTINCT routes_tbl.dst_airport_id) AS size
        FROM 
        routes_tbl
        GROUP BY airline_id
        ORDER BY size DESC
    ) counts_tbl
    LEFT JOIN airlines_tbl ON counts_tbl.airline_id = airlines_tbl.airline_id
"""
counts_airline_dst_airports = spark.sql(counts_airline_dst_airports_sql)
counts_airline_dst_airports.show(10)
    
# Q1 (d) Find all of the airlines that fly in and out of Auckland Airport, Auckland, New Zealand.
routes.registerTempTable('routes_tbl')
airlines.registerTempTable('airlines_tbl')

airlines_ack_sql = """
    SELECT 
    DISTINCT airlines_tbl.airline_name AS airline_name
    FROM
    routes_tbl
    LEFT JOIN airlines_tbl ON routes_tbl.airline_id = airlines_tbl.airline_id
    WHERE 
    routes_tbl.src_airport = 'AKL' OR routes_tbl.dst_airport = 'AKL'
"""
airlines_ack = spark.sql(airlines_ack_sql)
airlines_ack.show(airlines_ack.count(), False)

# Q1 (e) Find the airport which has the most routes that cross the equator (in either direction).
routes.registerTempTable('routes_tbl')
airports.registerTempTable('airports_tbl')

# Source airports rank
airport_cross_sql = """
    SELECT
    airports_tbl.airport AS airport_name,
    count(cross_equator_tbl.cross_airport_id) AS size
    FROM
    (
        SELECT
        src_tbl.src_airport_id AS airport_id,
        src_tbl.dst_airport_id AS cross_airport_id
        FROM
        (
            SELECT
            routes_tbl.src_airport_id AS src_airport_id,
            airports_tbl.latitude AS src_latitude,
            routes_tbl.dst_airport_id AS dst_airport_id
            FROM
            routes_tbl
            LEFT JOIN airports_tbl ON routes_tbl.src_airport_id = airports_tbl.airport_id
        ) src_tbl
        LEFT JOIN airports_tbl ON src_tbl.dst_airport_id = airports_tbl.airport_id
        WHERE src_tbl.src_latitude * airports_tbl.latitude < 0   
    ) cross_equator_tbl
    LEFT JOIN airports_tbl ON cross_equator_tbl.airport_id = airports_tbl.airport_id
    GROUP BY airport_name
    ORDER BY size DESC
"""

airport_cross = spark.sql(airport_cross_sql)
airport_cross.show(1)

# Distination airports rank
airport_cross_sql = """
    SELECT
    airports_tbl.airport AS airport_name,
    count(cross_equator_tbl.src_airport_id) AS size
    FROM
    (
        SELECT
        src_tbl.src_airport_id AS src_airport_id,
        src_tbl.dst_airport_id AS dst_airport_id
        FROM
        (
            SELECT
            routes_tbl.src_airport_id AS src_airport_id,
            airports_tbl.latitude AS src_latitude,
            routes_tbl.dst_airport_id AS dst_airport_id
            FROM
            routes_tbl
            LEFT JOIN airports_tbl ON routes_tbl.src_airport_id = airports_tbl.airport_id
        ) src_tbl
        LEFT JOIN airports_tbl ON src_tbl.dst_airport_id = airports_tbl.airport_id
        WHERE src_tbl.src_latitude * airports_tbl.latitude < 0   
    ) cross_equator_tbl
    LEFT JOIN airports_tbl ON cross_equator_tbl.dst_airport_id = airports_tbl.airport_id
    GROUP BY airport_name
    ORDER BY size DESC
"""

airport_cross = spark.sql(airport_cross_sql)
airport_cross.show(1)