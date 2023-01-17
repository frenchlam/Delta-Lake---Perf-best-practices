# Databricks notebook source
# using the Airline Dataset as the basis ~ 1.2 Billion lines 
display(dbutils.fs.ls("dbfs:/databricks-datasets/airlines/"))

# COMMAND ----------

# create the database
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
query = "CREATE DATABASE IF not exists flights_perf LOCATION 'dbfs:/Users/{user}/flights_perf_db'".format(user = current_user)
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a table over CSV files 
# MAGIC create table flights_perf.FLIGHTS_RAW (
# MAGIC       Year integer,
# MAGIC       Month integer,
# MAGIC       DayofMonth integer, 
# MAGIC       DayOfWeek integer, 
# MAGIC       DepTime string, 
# MAGIC       CRSDepTime integer, 
# MAGIC       ArrTime string, 
# MAGIC       CRSArrTime integer, 
# MAGIC       UniqueCarrier string, 
# MAGIC       FlightNum integer, 
# MAGIC       TailNum string, 
# MAGIC       ActualElapsedTime string, 
# MAGIC       CRSElapsedTime integer, 
# MAGIC       AirTime string, 
# MAGIC       ArrDelay string, 
# MAGIC       DepDelay string, 
# MAGIC       Origin string, 
# MAGIC       Dest string, 
# MAGIC       Distance string, 
# MAGIC       TaxiIn string, 
# MAGIC       TaxiOut string, 
# MAGIC       Cancelled integer, 
# MAGIC       CancellationCode string, 
# MAGIC       Diverted integer, 
# MAGIC       CarrierDelay string, 
# MAGIC       WeatherDelay string, 
# MAGIC       NASDelay string, 
# MAGIC       SecurityDelay string, 
# MAGIC       LateAircraftDelay string, 
# MAGIC       IsArrDelayed string, 
# MAGIC       IsDepDelayed string )
# MAGIC USING CSV 
# MAGIC LOCATION 'dbfs:/databricks-datasets/airlines/'
# MAGIC OPTIONS ('header' = TRUE )

# COMMAND ----------

initial_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/databricks-datasets/airlines/part-00000"))
df_schema = initial_df.schema

df = (spark.read
      .option("header", "false")
      .schema(df_schema)
      .csv("dbfs:/databricks-datasets/airlines/"))

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

#create a partitioned table - don't optimise 
df.write \
  .partitionBy("year") \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("maxRecordsPerFile", 200000) \
  .saveAsTable("flights_perf.FLIGHT_partitioned")

# .option("maxRecordsPerFile", 200000) \ # to simulate a small file prb

# COMMAND ----------

#create the table in Delta and optimise it
df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .saveAsTable("flights_perf.FLIGHT_optim")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE flights_perf.FLIGHT_optim

# COMMAND ----------

# create a table that is optimied and zordered
df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .saveAsTable("flights_perf.FLIGHT_optim_zorder")

# Auto-tune : https://docs.databricks.com/delta/tune-file-size.html#autotune-file-size-based-on-table-size

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- optimize table 
# MAGIC OPTIMIZE flights_perf.FLIGHT_optim_zorder ZORDER BY (year, Month, Origin) ; 
# MAGIC --Get column stats 
# MAGIC ANALYZE TABLE flights_perf.FLIGHT_optim_zorder COMPUTE STATISTICS FOR ALL COLUMNS

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(1) 
# MAGIC --FROM MATTHIEU_DB.FLIGHTS_RAW
# MAGIC --FROM MATTHIEU_DB.FLIGHT_partitioned
# MAGIC --FROM MATTHIEU_DB.FLIGHT_optim
# MAGIC --FROM MATTHIEU_DB.FLIGHT_optim_zorder

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- to be executed in DB SQL
# MAGIC 
# MAGIC SET use_cached_result = false ; 
# MAGIC 
# MAGIC -- Airlines with the most cancelled flight 
# MAGIC --     orginating from ORD (Chicago)
# MAGIC --     for the month of March 
# MAGIC 
# MAGIC with cancelled_origin as (
# MAGIC   select year, `Month`, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER (PARTITION BY year, Month ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   --FROM flights_perf.FLIGHTS_RAW -- Slow, really slow 
# MAGIC   --FROM flights_perf.FLIGHT_partitioned -- Better 
# MAGIC   --FROM flights_perf.FLIGHT_optim -- Even Better and we keep more flexible query performance around different filters
# MAGIC   FROM  flights_perf.FLIGHT_optim_zorder -- BEST and most flexible too!!
# MAGIC   WHERE month = 3 and Origin = "ORD"
# MAGIC   GROUP BY year, month, UniqueCarrier
# MAGIC )
# MAGIC SELECT year, `Month`, UniqueCarrier, nb_cancelled, percent_cancelled, canceled_rank
# MAGIC FROM cancelled_origin
# MAGIC where canceled_rank <= 3
