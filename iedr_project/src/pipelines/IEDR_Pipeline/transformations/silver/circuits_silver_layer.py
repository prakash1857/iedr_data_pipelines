from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog", "iedr")

@dp.materialized_view(
    name=f"{catalog}.silver.circuits",
    comment="Normalized circuits table - common schema across all utilities"
)
@dp.expect_or_drop("valid_circuit_id", "circuit_id IS NOT NULL")
@dp.expect_or_drop("valid_capacity", "max_hosting_capacity_mw IS NOT NULL")
def circuits():
    """
    Normalize and union circuits from both utilities to a common schema.
    Reads from bronze tables in batch mode for aggregation.
    
    Uses Materialized View because:
    - Utility 1 requires segment-to-circuit aggregation (groupBy)
    - Streaming aggregations with unions are incompatible with Auto CDC
    - Serverless MV provides automatic incremental refresh
    
    - Utility 1: Aggregates segment-level data to circuit/feeder level
    - Utility 2: Already at circuit level, just normalizes field names
    """
    
    # ========== UTILITY 1: AGGREGATE SEGMENTS TO CIRCUIT LEVEL ==========
    u1_bronze = spark.read.table(f"{catalog}.bronze.utility1_circuits")
    
    # Clean and filter out null/empty feeder IDs
    u1_cleaned = u1_bronze.withColumn("feeder_id", trim(col("NYHCPV_csv_NFEEDER"))) \
        .filter(col("feeder_id").isNotNull() & (col("feeder_id") != ""))
    
    # Aggregate segments to circuit/feeder level
    u1_circuits = u1_cleaned.groupBy("feeder_id", "utility_id", "ingestion_date") \
        .agg(
            max("NYHCPV_csv_FMAXHC").alias("max_hosting_capacity_mw"),
            min("NYHCPV_csv_FMINHC").alias("min_hosting_capacity_mw"),
            max("NYHCPV_csv_FVOLTAGE").alias("voltage_kv"),
            max("NYHCPV_csv_FHCADATE").alias("hca_refresh_date_raw"),
            sum("Shape_Length").alias("shape_length"),
            countDistinct("NYHCPV_csv_NSECTION").alias("num_segments")
        ) \
        .select(
            col("feeder_id").alias("circuit_id"),
            col("utility_id"),
            col("voltage_kv"),
            col("max_hosting_capacity_mw"),
            col("min_hosting_capacity_mw"),
            col("hca_refresh_date_raw").cast("date").alias("hca_refresh_date"),
            lit("Utility 1").alias("data_source"),
            col("shape_length"),
            col("num_segments"),
            col("ingestion_date")
        )
    
    # ========== UTILITY 2: NORMALIZE CIRCUIT-LEVEL DATA ==========
    u2_bronze = spark.read.table(f"{catalog}.bronze.utility2_circuits")
    
    # Clean and normalize
    u2_cleaned = u2_bronze.withColumn("circuit_id", trim(col("Master_CDF"))) \
        .filter(col("circuit_id").isNotNull() & (col("circuit_id") != ""))
    
    # Parse date from string format "yyyy/MM/dd HH:mm:ssX"
    u2_with_date = u2_cleaned.withColumn(
        "hca_refresh_date",
        to_date(to_timestamp(col("hca_refresh_date"), "yyyy/MM/dd HH:mm:ssX"))
    )
    
    # Deduplicate - keep most recent record per circuit
    window_spec = Window.partitionBy("circuit_id").orderBy(col("ingestion_date").desc())
    u2_deduplicated = u2_with_date.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Select and rename to common schema
    u2_circuits = u2_deduplicated.select(
        col("circuit_id"),
        col("utility_id"),
        col("feeder_voltage").alias("voltage_kv"),
        col("feeder_max_hc").alias("max_hosting_capacity_mw"),
        col("feeder_min_hc").alias("min_hosting_capacity_mw"),
        col("hca_refresh_date"),
        lit("Utility 2").alias("data_source"),
        col("shape_length"),
        lit(1).alias("num_segments"),  # Utility 2 already at circuit level
        col("ingestion_date")
    )
    
    # ========== UNION AND ADD METADATA ==========
    df_combined = u1_circuits.union(u2_circuits) \
        .withColumn("silver_transformation_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag",
                   when((col("circuit_id").isNotNull()) &
                        (col("max_hosting_capacity_mw").isNotNull()),
                        "VALID")
                   .otherwise("INVALID"))
    
    return df_combined
