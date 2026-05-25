from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ast

catalog = spark.conf.get("catalog", "iedr")

@dp.temporary_view()
def install_der_normalized():
    """
    Normalize and union installed DER data from all utilities to a common schema.
    Reads from streaming bronze tables for Auto CDC flow.
    
    Deduplication is handled by Auto CDC (SCD Type 1) based on keys and sequence_by.
    
    Common schema includes:
    - project_id: Unique identifier for the DER installation
    - project_type: Type of DER system (RESPHOTO, NRESPHOTO, Solar, etc.)
    - nameplate_rating_mw: Installed capacity in MW
    - circuit_id: Associated circuit/feeder identifier
    - interconnection_cost: Total interconnection costs
    - technology_breakdown_json: JSON with detailed technology mix (Utility 1 only)
    - service_address: Installation address (Utility 2 only)
    """
    
    # ========== UTILITY 1: NORMALIZE INSTALLED DER DATA ==========
    # Read from streaming bronze table
    u1_bronze = spark.readStream.table(f"{catalog}.bronze.utility1_install_der")
    
    u1_normalized = u1_bronze.select(
        col("ProjectID").alias("project_id"),
        col("ProjectType").alias("project_type"),
        col("NamePlateRating").alias("nameplate_rating_mw"),
        col("ProjectCircuitID").alias("circuit_id"),
        (coalesce(col("TotalChargesCESIR"), lit(0)) + 
         coalesce(col("TotalChargesConstruction"), lit(0))).alias("interconnection_cost"),
        col("CESIR_EST").alias("cesir_estimate"),
        col("SystemUpgrade_EST").alias("system_upgrade_estimate"),
        lit(None).cast("string").alias("service_address"),
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
        lit("Utility 1").alias("data_source"),
        col("utility_id"),
        col("ingestion_date")
    ) \
    .filter(col("ProjectID").isNotNull() & (trim(col("ProjectID")) != ""))
    
    # ========== UTILITY 2: NORMALIZE INSTALLED DER DATA ==========
    # Read from streaming bronze table
    u2_bronze = spark.readStream.table(f"{catalog}.bronze.utility2_install_der")
    
    u2_normalized = u2_bronze.select(
        col("DER_ID").alias("project_id"),
        col("DER_TYPE").alias("project_type"),
        col("DER_NAMEPLATE_RATING").alias("nameplate_rating_mw"),
        col("DER_INTERCONNECTION_LOCATION").alias("circuit_id"),
        col("INTERCONNECTION_COST").alias("interconnection_cost"),
        lit(None).cast("double").alias("cesir_estimate"),
        lit(None).cast("double").alias("system_upgrade_estimate"),
        col("SERVICE_STREET_ADDRESS").alias("service_address"),
        lit(None).cast("string").alias("is_hybrid"),
        lit(None).cast("string").alias("technology_breakdown_json"),  # Utility 2 doesn't have detailed breakdown
        lit("Utility 2").alias("data_source"),
        col("utility_id"),
        col("ingestion_date")
    ) \
    .filter(col("DER_ID").isNotNull() & (trim(col("DER_ID")) != ""))
    
    # ========== UNION UTILITIES AND ADD METADATA ==========
    df_combined = u1_normalized.union(u2_normalized)
    
    # Add metadata and data quality flag
    df_final = df_combined \
        .withColumn("silver_transformation_timestamp", current_timestamp()) \
        .withColumn("data_quality_flag",
                   when((col("project_id").isNotNull()) &
                        (col("nameplate_rating_mw").isNotNull()) &
                        (col("nameplate_rating_mw") > 0),
                        "VALID")
                   .otherwise("INVALID"))
    
    return df_final


# Create streaming table for SCD Type 1 target
dp.create_streaming_table(
    name=f"{catalog}.silver.install_der",
    comment="Normalized installed DER table with SCD Type 1 - tracks latest state per project"
)

# Create Auto CDC flow to maintain SCD Type 1 (handles deduplication automatically)
dp.create_auto_cdc_flow(
    target=f"{catalog}.silver.install_der",
    source="install_der_normalized",
    keys=["project_id", "utility_id"],
    sequence_by="ingestion_date",
    stored_as_scd_type=1
)
