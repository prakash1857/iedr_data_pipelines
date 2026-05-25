from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ast
from schemas.bronze_schemas import ddl_schema_map

# Retrieve configuration parameters from pipeline settings
catalog = spark.conf.get("catalog", "iedr")
table_list = spark.conf.get("table_list", "['circuits', 'planned_der', 'install_der']")
utility_list = spark.conf.get("utility_list", "['utility1', 'utility2']")  # Default to both utilities

# Parse configuration strings to Python lists
data_sources = ast.literal_eval(table_list)
utilities = ast.literal_eval(utility_list)


# Dynamic loop through all utilities and tables
for utility in utilities:
    utility_id = utility.replace("utility", "")  # Extract utility number (1, 2, 3, etc.)
    
    for table_name in data_sources:
        # Construct S3 path for this utility-table combination
        base_path = f"s3://iedr-pps/{utility}/{table_name}/"
        
        # Get DDL schema for this utility-table combination
        schema_key = f"{utility}_{table_name}_schema"
        
        # Skip if schema not defined for this utility-table combination
        if schema_key not in ddl_schema_map:
            print(f"Warning: Schema not found for {schema_key}, skipping...")
            continue
        
        schema_ddl = ddl_schema_map[schema_key]
        
        # Create streaming table using decorator
        @dp.table(
            name=f"{catalog}.bronze.{utility}_{table_name}",
            comment=f"{utility}_{table_name} data (Bronze layer) - Auto Loader streaming ingestion"
        )
        def create_streaming_table(
            path=base_path, 
            schema_hint=schema_ddl, 
            table=table_name,
            util_id=utility_id
        ):
            """
            Streaming table using Auto Loader for incremental file ingestion.
            Automatically detects and processes new CSV files uploaded to S3.
            Uses directory listing mode (no special AWS permissions required).
            
            This function is dynamically generated for each utility-table combination.
            """
            df = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("cloudFiles.schemaHints", schema_hint)
                .option("cloudFiles.inferColumnTypes", "true")
                .load(path)
                .withColumn("utility_id", lit(util_id))
                .withColumn("data_type", lit(table))
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("ingestion_date", current_date())
            )
            return df
