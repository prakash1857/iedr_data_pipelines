from pyspark.sql.types import *

# ============================================================================
# DDL SCHEMA DEFINITIONS (for Auto Loader cloudFiles.schemaHints)
# ============================================================================

# Utility 1 DDL Schemas
utility1_circuits_ddl = """
    row_num int,
    Circuits_Phase3_CIRCUIT string,
    Circuits_Phase3_NUMPHASES int,
    Circuits_Phase3_OVERUNDER string,
    Circuits_Phase3_PHASE string,
    NYHCPV_csv_NSECTION string,
    NYHCPV_csv_NFEEDER string,
    NYHCPV_csv_NVOLTAGE double,
    NYHCPV_csv_NMAXHC double,
    NYHCPV_csv_NMAPCOLOR string,
    NYHCPV_csv_FFEEDER string,
    NYHCPV_csv_FVOLTAGE double,
    NYHCPV_csv_FMAXHC double,
    NYHCPV_csv_FMINHC double,
    NYHCPV_csv_FHCADATE timestamp,
    NYHCPV_csv_FNOTES string,
    Shape_Length double
"""

utility1_install_der_ddl = """
    ProjectID string,
    ProjectType string,
    NamePlateRating double,
    TotalChargesCESIR double,
    TotalChargesConstruction double,
    CESIR_EST double,
    SystemUpgrade_EST double,
    ProjectCircuitID string,
    Hybrid string,
    SolarPV double,
    EnergyStorageSystem double,
    Wind double,
    MicroTurbine double,
    SynchronousGenerator double,
    InductionGenerator double,
    FarmWaste double,
    FuelCell double,
    CombinedHeatandPower double,
    GasTurbine double,
    Hydro double,
    InternalCombustionEngine double,
    SteamTurbine double,
    Other double
"""

utility1_planned_der_ddl = """
    ProjectType string,
    NamePlateRating double,
    InServiceDate string,
    ProjectStatus string,
    ProjectID string,
    CompletionDate string,
    ProjectCircuitID string,
    Hybrid string,
    SolarPV double,
    EnergyStorageSystem double,
    Wind double,
    MicroTurbine double,
    SynchronousGenerator double,
    InductionGenerator double,
    FarmWaste double,
    FuelCell double,
    CombinedHeatandPower double,
    GasTurbine double,
    Hydro double,
    InternalCombustionEngine double,
    SteamTurbine double,
    Other double
"""

# Utility 2 DDL Schemas
utility2_circuits_ddl = """
    Master_CDF string,
    feeder_voltage double,
    feeder_max_hc double,
    feeder_min_hc double,
    feeder_dg_connected_since_refresh double,
    hca_refresh_date string,
    color string,
    shape_length double
"""

utility2_install_der_ddl = """
    DER_ID string,
    SERVICE_STREET_ADDRESS string,
    DER_TYPE string,
    DER_NAMEPLATE_RATING double,
    DER_INTERCONNECTION_LOCATION string,
    INTERCONNECTION_COST double
"""

utility2_planned_der_ddl = """
    DER_TYPE string,
    DER_NAMEPLATE_RATING double,
    INVERTER_NAMEPLATE_RATING double,
    PLANNED_INSTALLATION_DATE string,
    DER_STATUS string,
    DER_STATUS_RATIONALE string,
    TOTAL_MW_FOR_SUBSTATION double,
    INTERCONNECTION_QUEUE_REQUEST_ID string,
    INTERCONNECTION_QUEUE_POSITION string,
    DER_INTERCONNECTION_LOCATION string
"""

# DDL Schema map - maps schema names to DDL strings
ddl_schema_map = {
    "utility1_circuits_schema": utility1_circuits_ddl,
    "utility1_install_der_schema": utility1_install_der_ddl,
    "utility1_planned_der_schema": utility1_planned_der_ddl,
    "utility2_circuits_schema": utility2_circuits_ddl,
    "utility2_install_der_schema": utility2_install_der_ddl,
    "utility2_planned_der_schema": utility2_planned_der_ddl
}


# # ============================================================================
# # STRUCTTYPE SCHEMA DEFINITIONS (for backward compatibility)
# # ============================================================================

# # Utility 1 Schemas
# utility1_circuits_schema = StructType([
#     StructField("row_num", IntegerType(), True),
#     StructField("Circuits_Phase3_CIRCUIT", StringType(), True),
#     StructField("Circuits_Phase3_NUMPHASES", IntegerType(), True),
#     StructField("Circuits_Phase3_OVERUNDER", StringType(), True),
#     StructField("Circuits_Phase3_PHASE", StringType(), True),
#     StructField("NYHCPV_csv_NSECTION", StringType(), True),
#     StructField("NYHCPV_csv_NFEEDER", StringType(), True),
#     StructField("NYHCPV_csv_NVOLTAGE", DoubleType(), True),
#     StructField("NYHCPV_csv_NMAXHC", DoubleType(), True),
#     StructField("NYHCPV_csv_NMAPCOLOR", StringType(), True),
#     StructField("NYHCPV_csv_FFEEDER", StringType(), True),
#     StructField("NYHCPV_csv_FVOLTAGE", DoubleType(), True),
#     StructField("NYHCPV_csv_FMAXHC", DoubleType(), True),
#     StructField("NYHCPV_csv_FMINHC", DoubleType(), True),
#     StructField("NYHCPV_csv_FHCADATE", TimestampType(), True),
#     StructField("NYHCPV_csv_FNOTES", StringType(), True),
#     StructField("Shape_Length", DoubleType(), True)
# ])

# utility1_install_der_schema = StructType([
#     StructField("ProjectID", StringType(), True),
#     StructField("ProjectType", StringType(), True),
#     StructField("NamePlateRating", DoubleType(), True),
#     StructField("TotalChargesCESIR", DoubleType(), True),
#     StructField("TotalChargesConstruction", DoubleType(), True),
#     StructField("CESIR_EST", DoubleType(), True),
#     StructField("SystemUpgrade_EST", DoubleType(), True),
#     StructField("ProjectCircuitID", StringType(), True),
#     StructField("Hybrid", StringType(), True),
#     StructField("SolarPV", DoubleType(), True),
#     StructField("EnergyStorageSystem", DoubleType(), True),
#     StructField("Wind", DoubleType(), True),
#     StructField("MicroTurbine", DoubleType(), True),
#     StructField("SynchronousGenerator", DoubleType(), True),
#     StructField("InductionGenerator", DoubleType(), True),
#     StructField("FarmWaste", DoubleType(), True),
#     StructField("FuelCell", DoubleType(), True),
#     StructField("CombinedHeatandPower", DoubleType(), True),
#     StructField("GasTurbine", DoubleType(), True),
#     StructField("Hydro", DoubleType(), True),
#     StructField("InternalCombustionEngine", DoubleType(), True),
#     StructField("SteamTurbine", DoubleType(), True),
#     StructField("Other", DoubleType(), True)
# ])

# utility1_planned_der_schema = StructType([
#     StructField("ProjectType", StringType(), True),
#     StructField("NamePlateRating", DoubleType(), True),
#     StructField("InServiceDate", StringType(), True),
#     StructField("ProjectStatus", StringType(), True),
#     StructField("ProjectID", StringType(), True),
#     StructField("CompletionDate", StringType(), True),
#     StructField("ProjectCircuitID", StringType(), True),
#     StructField("Hybrid", StringType(), True),
#     StructField("SolarPV", DoubleType(), True),
#     StructField("EnergyStorageSystem", DoubleType(), True),
#     StructField("Wind", DoubleType(), True),
#     StructField("MicroTurbine", DoubleType(), True),
#     StructField("SynchronousGenerator", DoubleType(), True),
#     StructField("InductionGenerator", DoubleType(), True),
#     StructField("FarmWaste", DoubleType(), True),
#     StructField("FuelCell", DoubleType(), True),
#     StructField("CombinedHeatandPower", DoubleType(), True),
#     StructField("GasTurbine", DoubleType(), True),
#     StructField("Hydro", DoubleType(), True),
#     StructField("InternalCombustionEngine", DoubleType(), True),
#     StructField("SteamTurbine", DoubleType(), True),
#     StructField("Other", DoubleType(), True)
# ])

# # Utility 2 Schemas
# utility2_circuits_schema = StructType([
#     StructField("Master_CDF", StringType(), True),
#     StructField("feeder_voltage", DoubleType(), True),
#     StructField("feeder_max_hc", DoubleType(), True),
#     StructField("feeder_min_hc", DoubleType(), True),
#     StructField("feeder_dg_connected_since_refresh", DoubleType(), True),
#     StructField("hca_refresh_date", StringType(), True),
#     StructField("color", StringType(), True),
#     StructField("shape_length", DoubleType(), True)
# ])

# utility2_install_der_schema = StructType([
#     StructField("DER_ID", StringType(), True),
#     StructField("SERVICE_STREET_ADDRESS", StringType(), True),
#     StructField("DER_TYPE", StringType(), True),
#     StructField("DER_NAMEPLATE_RATING", DoubleType(), True),
#     StructField("DER_INTERCONNECTION_LOCATION", StringType(), True),
#     StructField("INTERCONNECTION_COST", DoubleType(), True),
# ])

# utility2_planned_der_schema = StructType([
#     StructField("DER_TYPE", StringType(), True),
#     StructField("DER_NAMEPLATE_RATING", DoubleType(), True),
#     StructField("INVERTER_NAMEPLATE_RATING", DoubleType(), True),
#     StructField("PLANNED_INSTALLATION_DATE", StringType(), True),
#     StructField("DER_STATUS", StringType(), True),
#     StructField("DER_STATUS_RATIONALE", StringType(), True),
#     StructField("TOTAL_MW_FOR_SUBSTATION", DoubleType(), True),
#     StructField("INTERCONNECTION_QUEUE_REQUEST_ID", StringType(), True),
#     StructField("INTERCONNECTION_QUEUE_POSITION", StringType(), True),
#     StructField("DER_INTERCONNECTION_LOCATION", StringType(), True)
# ])

# # Schema map - maps schema names to schema objects (backward compatibility)
# schema_map = {
#     "utility1_circuits_schema": utility1_circuits_schema,
#     "utility1_install_der_schema": utility1_install_der_schema,
#     "utility1_planned_der_schema": utility1_planned_der_schema,
#     "utility2_circuits_schema": utility2_circuits_schema,
#     "utility2_install_der_schema": utility2_install_der_schema,
#     "utility2_planned_der_schema": utility2_planned_der_schema
# }
