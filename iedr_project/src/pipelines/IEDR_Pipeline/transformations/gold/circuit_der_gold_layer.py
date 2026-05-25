from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = spark.conf.get("catalog", "iedr")

@dp.temporary_view()
def circuit_der_normalized():
    """
    Gold layer: Union of installed and planned DER data.
    Combines both sources with a common schema and status indicator.
    
    This creates a comprehensive view of all DER projects (installed + planned)
    associated with feeders/circuits for capacity planning and analysis.
    """
    
    # ========== READ INSTALLED DER DATA ==========
    install_der = spark.readStream.table("iedr.silver.install_der") \
        .select(
            col("project_id"),
            col("project_type"),
            col("nameplate_rating_mw"),
            col("circuit_id"),
            col("utility_id"),
            col("data_source"),
            col("ingestion_date"),
            
            # Installed DER specific fields
            col("interconnection_cost"),
            col("cesir_estimate"),
            col("system_upgrade_estimate"),
            col("service_address"),
            
            # Common fields
            #col("is_hybrid"),
            col("technology_breakdown_json"),
            col("data_quality_flag"),
            
            # Planned DER fields (set to NULL for installed)
            lit(None).cast("string").alias("project_status"),
            lit(None).cast("date").alias("planned_installation_date"),
            lit(None).cast("string").alias("completion_date"),
            lit(None).cast("timestamp").alias("queue_position"),
            lit(None).cast("double").alias("inverter_nameplate_rating_mw"),
            lit(None).cast("double").alias("total_mw_for_substation"),
            lit(None).cast("string").alias("status_rationale"),
            
            # Add status indicator
            lit("INSTALLED").alias("der_status")
        )
    
    # ========== READ PLANNED DER DATA ==========
    planned_der = spark.readStream.table("iedr.silver.planned_der") \
        .select(
            col("project_id"),
            col("project_type"),
            col("nameplate_rating_mw"),
            col("circuit_id"),
            col("utility_id"),
            col("data_source"),
            col("ingestion_date"),
            
            # Installed DER fields (set to NULL for planned)
            lit(None).cast("double").alias("interconnection_cost"),
            lit(None).cast("double").alias("cesir_estimate"),
            lit(None).cast("double").alias("system_upgrade_estimate"),
            lit(None).cast("string").alias("service_address"),
            
            # Common fields
           # col("is_hybrid"),
            col("technology_breakdown_json"),
            col("data_quality_flag"),
            
            # Planned DER specific fields
            col("project_status"),
            col("planned_installation_date"),
            col("completion_date"),
            col("queue_position"),
            col("inverter_nameplate_rating_mw"),
            col("total_mw_for_substation"),
            col("status_rationale"),
            
            # Add status indicator
            lit("PLANNED").alias("der_status")
        )
    
    # ========== UNION BOTH DATASETS ==========
    df_combined = install_der.union(planned_der)
    
    # ========== ADD GOLD LAYER METADATA ==========
    df_final = df_combined \
        .withColumn("gold_transformation_timestamp", current_timestamp()) \
        .withColumn("der_category",
                   when(col("der_status") == "INSTALLED", "OPERATIONAL")
                   .when((col("der_status") == "PLANNED") & 
                         (col("project_status").isin("APPROVED", "IN_PROGRESS")), "PIPELINE")
                   .otherwise("FUTURE")) \
        .withColumn("total_capacity_mw", col("nameplate_rating_mw"))
    
    return df_final


# Create streaming table for SCD Type 1 target
dp.create_streaming_table(
    name="iedr.gold.circuit_der",
    comment="Gold layer: Combined installed and planned DER projects with SCD Type 1 tracking"
)

# Create Auto CDC flow to maintain SCD Type 1
# Tracks latest state per project per utility
dp.create_auto_cdc_flow(
    target="iedr.gold.circuit_der",
    source="circuit_der_normalized",
    keys=["project_id", "utility_id"],
    sequence_by="ingestion_date",
    stored_as_scd_type=1
)
