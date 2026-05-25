from pyspark import pipelines as dp

catalog = spark.conf.get("catalog", "iedr")

@dp.temporary_view()
def circuits_for_scd():
    """
    Temporary view that reads circuits MV with skipChangeCommits option.
    This allows streaming reads from a materialized view that performs updates.
    """
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .table(f"{catalog}.silver.circuits")


# Create streaming table for SCD Type 1 target
dp.create_streaming_table(
    name=f"{catalog}.silver.circuits_scd",
    comment="Circuits SCD Type 1 table - tracks latest state per circuit with change history"
)

# Create Auto CDC flow to maintain SCD Type 1
# Reads from the temporary view that handles MV updates
dp.create_auto_cdc_flow(
    target=f"{catalog}.silver.circuits_scd",
    source="circuits_for_scd",
    keys=["circuit_id", "utility_id"],
    sequence_by="ingestion_date",
    stored_as_scd_type=1
)
