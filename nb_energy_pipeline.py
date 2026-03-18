# Databricks notebook source
storage_account = "stcarbondatalake01"
container = "raw"

path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/energy_readings/"

df = spark.read.format("csv") \
    .option("header", "true") \
    .load(path)

display(df)

# COMMAND ----------

df.printSchema()
df.show()

# COMMAND ----------

print("row count:", df.count())

# COMMAND ----------

from pyspark.sql.functions import col

df_clean = df.select(
    col("reading_time").cast("timestamp"),
    col("site_id"),
    col("building_id"),
    col("meter_id"),
    col("energy_kwh").cast("double"),
    col("gas_kwh").cast("double"),
    col("co2_kg").cast("double"),
    col("temperature_c").cast("double"),
    col("occupancy").cast("double")
)

# COMMAND ----------

df_clean.filter(col("occupancy").isNull()).show()

# COMMAND ----------

from pyspark.sql.functions import when, lit

df_flagged = df_clean.withColumn(
    "dq_energy_invalid",
    when(col("energy_kwh") < 0, lit(1)).otherwise(lit(0))
).withColumn(
    "dq_temp_invalid",
    when((col("temperature_c") < -20) | (col("temperature_c") > 60), lit(1)).otherwise(lit(0))
).withColumn(
    "dq_occupancy_missing",
    when(col("occupancy").isNull(), lit(1)).otherwise(lit(0))
)

df_flagged.show()

# COMMAND ----------

from pyspark.sql.functions import sum

df_flagged.select(
    sum("dq_energy_invalid").alias("bad_energy_rows"),
    sum("dq_temp_invalid").alias("bad_temp_rows"),
    sum("dq_occupancy_missing").alias("missing_occupancy_rows")
).show()

# COMMAND ----------

df_valid = df_flagged.filter(
    (col("dq_energy_invalid") == 0) &
    (col("dq_temp_invalid") == 0)
)

df_valid.show()
print("valid rows:", df_valid.count())

# COMMAND ----------

df_valid.write.mode("overwrite").parquet(
    "abfss://silver@stcarbondatalake01.dfs.core.windows.net/energy_clean/"
)

# COMMAND ----------

df_silver = spark.read.parquet(
    "abfss://silver@stcarbondatalake01.dfs.core.windows.net/energy_clean/"
)

df_silver.show()
print("silver row count:", df_silver.count())

# COMMAND ----------

from pyspark.sql.functions import to_date, sum, avg, count

df_gold = df_valid.withColumn("reading_date", to_date(col("reading_time"))) \
    .groupBy("reading_date", "site_id", "building_id") \
    .agg(
        sum("energy_kwh").alias("total_energy_kwh"),
        sum("gas_kwh").alias("total_gas_kwh"),
        sum("co2_kg").alias("total_co2_kg"),
        avg("temperature_c").alias("avg_temperature_c"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("reading_count")
    )

df_gold.show()

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_gold.write \
    .mode("overwrite") \
    .partitionBy("reading_date") \
    .parquet("abfss://gold@stcarbondatalake01.dfs.core.windows.net/site_daily_summary/")

# COMMAND ----------

df_gold_check = spark.read.parquet(
    "abfss://gold@stcarbondatalake01.dfs.core.windows.net/site_daily_summary/"
)

df_gold_check.show()
print("gold row count:", df_gold_check.count())

# COMMAND ----------

df_gold_check.select(
    "building_id",
    "total_energy_kwh",
    "avg_temperature_c"
).show()

# COMMAND ----------

df = spark.read.parquet("abfss://gold@stcarbondatalake01.dfs.core.windows.net/site_daily_summary/")
df.show()

# COMMAND ----------

df = spark.read.parquet("abfss://gold@stcarbondatalake01.dfs.core.windows.net/site_daily_summary/")
print("row count:", df.count())
df.show()