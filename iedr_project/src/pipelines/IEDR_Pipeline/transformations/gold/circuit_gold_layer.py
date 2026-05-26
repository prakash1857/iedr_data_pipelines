from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = spark.conf.get("catalog", "iedr")

@dp.temporary_view()
def circuit_normalized():
    """
    Gold layer: Join circuits_scd with installed and planned DER.
    Creates a comprehensive view with circuits as the driver, enriched with DER project information.
    """
    
    # ========== READ CIRCUITS (DRIVER - BATCH/CURRENT STATE) ==========
    # Read circuits_scd as batch since it's already an SCD table with current state
    # This enables stream-static join pattern (avoids stream-stream join requirements)
    df_circuits = spark.readStream.table(f"{catalog}.silver.circuits_scd") \
        .select(
            col("circuit_id"),
            col("utility_id"),
            col("voltage_kv"),
            col("max_hosting_capacity_mw"),
            col("min_hosting_capacity_mw"),
            col("hca_refresh_date"),
            col("data_source").alias("circuit_data_source"),
            col("shape_length"),
            col("num_segments"),
            col("data_quality_flag")
            col("ingestion_date")
        )
    
    # ========== Count of INSTALLED DER - FILTER VALID CIRCUIT_ID ==========
    df_installed_counts = spark.read.table(f"{catalog}.silver.install_der") \
        .filter((col("circuit_id").isNotNull()) & (trim(col("circuit_id")) != "")) \
        .groupBy("utility_id", "circuit_id")\
        .agg(count("*").alias("installed_der_count"))
    
    # ========== Count of PLANNED DER - FILTER VALID CIRCUIT_ID ==========
    df_planned_counts = spark.read.table(f"{catalog}.silver.planned_der") \
        .filter((col("circuit_id").isNotNull()) & (trim(col("circuit_id")) != "")) \
        .groupBy("utility_id", "circuit_id")\
        .agg(count("*").alias("planned_der_count"))
       
    # ========== Join Count into Circuit ==========
    
    df_joined = (
    df_circuits
    .join(df_installed_counts, on=["utility_id", "circuit_id"], how="left")
    .join(df_planned_counts,   on=["utility_id", "circuit_id"], how="left")
    .withColumn("installed_der_count", col("installed_der_count").cast("int"))
    .withColumn("planned_der_count", col("planned_der_count").cast("int"))
    .withColumn("last_refreshed", current_timestamp())
    )
    
    return df_joined


# Create streaming table for SCD Type 1 target
dp.create_streaming_table(
    name=f"{catalog}.gold.circuit",
    comment="Gold layer: Circuits enriched with DER project information (installed and planned), SCD Type 1 tracking"
)

# Create Auto CDC flow to maintain SCD Type 1
# Tracks latest state per project per utility
dp.create_auto_cdc_flow(
    target=f"{catalog}.gold.circuit",
    source="circuit_normalized",
    keys=["utility_id","circuit_id"],
    sequence_by="ingestion_date",
    stored_as_scd_type=1
)
