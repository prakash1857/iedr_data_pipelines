from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = spark.conf.get("catalog", "iedr")

# Step 1: Create temporary view with normalization logic (preprocessing)
@dp.temporary_view()
def planned_der_normalized():
    """
    Normalize and union planned DER data from all utilities to a common schema.
    This view serves as the source for Auto CDC flow.
    
    Streaming reads from bronze tables for continuous processing.
    """
    
    # ========== UTILITY 1: NORMALIZE PLANNED DER DATA ==========
    # Read from streaming bronze table
    u1_bronze = spark.readStream.table(f"{catalog}.bronze.utility1_planned_der")
    
    u1_normalized = u1_bronze.select(
        col("ProjectID").alias("project_id"),
        col("ProjectType").alias("project_type"),
        col("NamePlateRating").alias("nameplate_rating_mw"),
        col("ProjectCircuitID").alias("circuit_id"),
        col("ProjectStatus").alias("project_status"),
        col("InServiceDate").cast("date").alias("planned_installation_date"),
        col("CompletionDate").alias("completion_date"),
        col("Hybrid").alias("is_hybrid"),
        # Technology breakdown as JSON string for detailed analysis
        to_json(struct(
            col("SolarPV").alias("solar_pv_mw"),
            col("EnergyStorageSystem").alias("energy_storage_mw"),
            col("Wind").alias("wind_mw"),
            col("MicroTurbine").alias("micro_turbine_mw"),
            col("SynchronousGenerator").alias("synchronous_gen_mw"),
            col("InductionGenerator").alias("induction_gen_mw"),
            col("FarmWaste").alias("farm_waste_mw"),
            col("FuelCell").alias("fuel_cell_mw"),
            col("CombinedHeatandPower").alias("chp_mw"),
            col("GasTurbine").alias("gas_turbine_mw"),
            col("Hydro").alias("hydro_mw"),
            col("InternalCombustionEngine").alias("ice_mw"),
            col("SteamTurbine").alias("steam_turbine_mw"),
            col("Other").alias("other_mw")
        )).alias("technology_breakdown_json"),
        lit(None).cast("timestamp").alias("queue_position"),
        lit(None).cast("double").alias("inverter_nameplate_rating_mw"),
        lit(None).cast("double").alias("total_mw_for_substation"),
        lit(None).cast("string").alias("status_rationale"),
        lit("Utility 1").alias("data_source"),
        col("utility_id"),
        col("ingestion_date")
    )
    
    # ========== UTILITY 2: NORMALIZE PLANNED DER DATA ==========
    # Read from streaming bronze table
    u2_bronze = spark.readStream.table(f"{catalog}.bronze.utility2_planned_der")
    
    u2_normalized = u2_bronze.select(
        col("INTERCONNECTION_QUEUE_REQUEST_ID").alias("project_id"),
        col("DER_TYPE").alias("project_type"),
        col("DER_NAMEPLATE_RATING").alias("nameplate_rating_mw"),
        col("DER_INTERCONNECTION_LOCATION").alias("circuit_id"),
        col("DER_STATUS").alias("project_status"),
        # Parse date from string format "MM/dd/yyyy" to date
        to_date(col("PLANNED_INSTALLATION_DATE"), "MM/dd/yyyy").alias("planned_installation_date"),
        lit(None).cast("string").alias("completion_date"),
        lit(None).cast("string").alias("is_hybrid"),
        lit(None).cast("string").alias("technology_breakdown_json"),  # Utility 2 doesn't have detailed breakdown
        # Parse timestamp from string format "MM/dd/yyyy HH:mm" to timestamp
        to_timestamp(col("INTERCONNECTION_QUEUE_POSITION"), "MM/dd/yyyy HH:mm").alias("queue_position"),
        col("INVERTER_NAMEPLATE_RATING").alias("inverter_nameplate_rating_mw"),
        col("TOTAL_MW_FOR_SUBSTATION").alias("total_mw_for_substation"),
        col("DER_STATUS_RATIONALE").alias("status_rationale"),
        lit("Utility 2").alias("data_source"),
        col("utility_id"),
        col("ingestion_date")
    )
    
    # ========== UNION UTILITIES AND ADD METADATA ==========
    df_combined = u1_normalized.union(u2_normalized)
    
    # Add metadata and data quality flags
    df_final = df_combined \
        .withColumn("silver_transformation_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag",
                   when((col("project_id").isNotNull()) &
                        (col("nameplate_rating_mw").isNotNull()) &
                        (col("nameplate_rating_mw") > 0),
                        "VALID")
                   .otherwise("INVALID"))
    
    return df_final


# Step 2: Create target streaming table (renamed to avoid conflict with existing MATERIALIZED_VIEW)
dp.create_streaming_table(
    name=f"{catalog}.silver.planned_der",
    comment="Normalized planned DER table - SCD Type 1 with Auto CDC (replaces planned_der)"
)


# Step 3: Define Auto CDC flow (SCD Type 1 - overwrites on update)
dp.create_auto_cdc_flow(
    target=f"{catalog}.silver.planned_der",
    source="planned_der_normalized",
    keys=["project_id", "utility_id"],  # Composite key: unique per project per utility
    sequence_by="ingestion_date",       # Order by ingestion date for change tracking
    stored_as_scd_type=1               # SCD Type 1: keep only latest version
)
