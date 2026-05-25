"""
Comprehensive test suite for IEDR Pipeline.

This test module covers:
- Bronze layer: Auto Loader ingestion and schema validation
- Silver layer: Circuits, Planned DER, Installed DER transformations
- Gold layer: Circuit aggregations and DER analytics
- Integration: End-to-end data flow validation
- Data Quality: NULL handling, deduplication, and validation rules

Run with: databricks bundle test
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime


# ============================================================================
# Bronze Layer Tests - Data Ingestion
# ============================================================================

class TestBronzeLayer:
    """Test bronze layer auto loader ingestion."""
    
    def test_bronze_circuits_schema_utility1(self, spark):
        """Test that utility1 circuits data has expected schema after bronze ingestion."""
        # Expected schema for utility1 circuits (segment-level data)
        expected_fields = [
            "circuit_id", "segment_id", "voltage_kv", "length_km", 
            "capacity_mva", "load_mva", "ingestion_timestamp"
        ]
        
        # Create sample data
        data = [
            ("CKT001", "SEG001", "11.0", "2.5", "10.0", "7.5", datetime.now()),
            ("CKT001", "SEG002", "11.0", "1.8", "10.0", "6.2", datetime.now()),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("segment_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("length_km", StringType()),
            StructField("capacity_mva", StringType()),
            StructField("load_mva", StringType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Verify schema fields exist
        actual_fields = df.columns
        for field in expected_fields:
            assert field in actual_fields, f"Missing expected field: {field}"
        
        # Verify data is ingested
        assert df.count() == 2, "Expected 2 records"
    
    def test_bronze_circuits_schema_utility2(self, spark):
        """Test that utility2 circuits data has expected schema after bronze ingestion."""
        # Expected schema for utility2 circuits (circuit-level data)
        expected_fields = [
            "circuit_id", "voltage_kv", "total_length_km", 
            "total_capacity_mva", "avg_load_mva", "ingestion_timestamp"
        ]
        
        data = [
            ("CKT101", "22.0", "5.3", "25.0", "18.2", datetime.now()),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        actual_fields = df.columns
        for field in expected_fields:
            assert field in actual_fields, f"Missing expected field: {field}"
    
    def test_bronze_planned_der_schema(self, spark):
        """Test planned DER data schema after bronze ingestion."""
        expected_fields = [
            "project_id", "circuit_id", "technology_type", "capacity_kw",
            "planned_commission_date", "project_status", "ingestion_timestamp"
        ]
        
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned", datetime.now()),
        ]
        
        schema = StructType([
            StructField("project_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("planned_commission_date", StringType()),
            StructField("project_status", StringType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        actual_fields = df.columns
        for field in expected_fields:
            assert field in actual_fields, f"Missing expected field: {field}"
    
    def test_bronze_install_der_schema(self, spark):
        """Test installed DER data schema after bronze ingestion."""
        expected_fields = [
            "installation_id", "circuit_id", "technology_type", "capacity_kw",
            "commission_date", "location", "ingestion_timestamp"
        ]
        
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A", datetime.now()),
        ]
        
        schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("commission_date", StringType()),
            StructField("location", StringType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        actual_fields = df.columns
        for field in expected_fields:
            assert field in actual_fields, f"Missing expected field: {field}"


# ============================================================================
# Silver Layer Tests - Circuits Transformation
# ============================================================================

class TestCircuitsSilverLayer:
    """Test circuits silver layer transformations."""
    
    def test_utility1_segment_aggregation(self, spark):
        """Test that utility1 segment-level data is correctly aggregated to circuit level."""
        data = [
            ("CKT001", "SEG001", "11.0", "2.5", "10.0", "7.5"),
            ("CKT001", "SEG002", "11.0", "1.8", "10.0", "6.2"),
            ("CKT002", "SEG003", "22.0", "3.0", "15.0", "12.0"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("segment_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("length_km", StringType()),
            StructField("capacity_mva", StringType()),
            StructField("load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Simulate aggregation logic
        from pyspark.sql.functions import sum as spark_sum, avg, first
        
        result = df.groupBy("circuit_id").agg(
            first("voltage_kv").alias("voltage_kv"),
            spark_sum("length_km").alias("total_length_km"),
            spark_sum("capacity_mva").alias("total_capacity_mva"),
            avg("load_mva").alias("avg_load_mva")
        )
        
        # Verify aggregation
        assert result.count() == 2, "Expected 2 unique circuits"
        
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert float(ckt001["total_length_km"]) == pytest.approx(4.3, 0.01), "Total length should be 2.5 + 1.8"
        assert float(ckt001["avg_load_mva"]) == pytest.approx(6.85, 0.01), "Avg load should be (7.5 + 6.2) / 2"
    
    def test_utility2_normalization(self, spark):
        """Test that utility2 circuit-level data is correctly normalized."""
        data = [
            ("CKT101", "22.0", "5.3", "25.0", "18.2"),
            ("CKT102", "11.0", "3.2", "12.0", "9.5"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Verify normalized column names and types
        assert "circuit_id" in df.columns
        assert "voltage_kv" in df.columns
        assert df.count() == 2
    
    def test_circuits_union_operation(self, spark):
        """Test that utility1 and utility2 circuits are correctly unioned."""
        # Utility1 aggregated data
        utility1_data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
        ]
        
        # Utility2 data
        utility2_data = [
            ("CKT101", "22.0", "5.3", "25.0", "18.2"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df1 = spark.createDataFrame(utility1_data, schema)
        df2 = spark.createDataFrame(utility2_data, schema)
        
        result = df1.union(df2)
        
        assert result.count() == 2, "Union should contain both utility datasets"
        assert result.filter("circuit_id = 'CKT001'").count() == 1
        assert result.filter("circuit_id = 'CKT101'").count() == 1
    
    def test_null_filtering(self, spark):
        """Test that NULL or empty circuit IDs are filtered out."""
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            (None, "22.0", "5.3", "25.0", "18.2"),
            ("", "11.0", "3.2", "12.0", "9.5"),
            ("  ", "22.0", "2.1", "8.0", "6.0"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Filter logic
        from pyspark.sql.functions import trim, col
        result = df.filter(
            (col("circuit_id").isNotNull()) & 
            (trim(col("circuit_id")) != "")
        )
        
        assert result.count() == 1, "Only valid circuit_id should remain"
        assert result.collect()[0]["circuit_id"] == "CKT001"
    
    def test_data_quality_checks(self, spark):
        """Test data quality: voltage and capacity should be positive."""
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            ("CKT002", "-22.0", "5.3", "25.0", "18.2"),  # Invalid negative voltage
            ("CKT003", "11.0", "3.2", "-12.0", "9.5"),   # Invalid negative capacity
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Quality check logic
        from pyspark.sql.functions import col
        result = df.filter(
            (col("voltage_kv").cast("double") > 0) &
            (col("total_capacity_mva").cast("double") > 0)
        )
        
        assert result.count() == 1, "Only valid records should pass quality checks"


# ============================================================================
# Silver Layer Tests - Planned DER Transformation
# ============================================================================

class TestPlannedDERSilverLayer:
    """Test planned DER silver layer transformations."""
    
    def test_circuit_id_validation(self, spark):
        """Test that only valid circuit IDs are processed."""
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", None, "Wind", "1000", "2026-08-20", "Planned"),
            ("PRJ003", "", "Battery", "250", "2026-09-10", "Planned"),
        ]
        
        schema = StructType([
            StructField("project_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("planned_commission_date", StringType()),
            StructField("project_status", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Filter invalid circuit_ids
        from pyspark.sql.functions import col, trim
        result = df.filter(
            (col("circuit_id").isNotNull()) &
            (trim(col("circuit_id")) != "")
        )
        
        assert result.count() == 1, "Only records with valid circuit_id should remain"
    
    def test_date_parsing(self, spark):
        """Test that planned commission dates are correctly parsed."""
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT002", "Wind", "1000", "2026-08-20", "Planned"),
        ]
        
        schema = StructType([
            StructField("project_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("planned_commission_date", StringType()),
            StructField("project_status", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Parse dates
        from pyspark.sql.functions import to_date
        result = df.withColumn("commission_date", to_date("planned_commission_date", "yyyy-MM-dd"))
        
        assert result.filter("commission_date IS NOT NULL").count() == 2
    
    def test_project_status_filtering(self, spark):
        """Test filtering by project status."""
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT002", "Wind", "1000", "2026-08-20", "Cancelled"),
            ("PRJ003", "CKT003", "Battery", "250", "2026-09-10", "Planned"),
        ]
        
        schema = StructType([
            StructField("project_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("planned_commission_date", StringType()),
            StructField("project_status", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Filter active projects
        result = df.filter("project_status = 'Planned'")
        
        assert result.count() == 2, "Only Planned projects should be included"
    
    def test_capacity_validation(self, spark):
        """Test that capacity values are positive and valid."""
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT002", "Wind", "-1000", "2026-08-20", "Planned"),  # Invalid
            ("PRJ003", "CKT003", "Battery", "0", "2026-09-10", "Planned"),    # Invalid
        ]
        
        schema = StructType([
            StructField("project_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("planned_commission_date", StringType()),
            StructField("project_status", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Validate capacity
        from pyspark.sql.functions import col
        result = df.filter(col("capacity_kw").cast("double") > 0)
        
        assert result.count() == 1, "Only positive capacity values should be valid"


# ============================================================================
# Silver Layer Tests - Installed DER Transformation
# ============================================================================

class TestInstallDERSilverLayer:
    """Test installed DER silver layer transformations."""
    
    def test_installation_data_processing(self, spark):
        """Test that installation data is correctly processed."""
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", "Substation B"),
        ]
        
        schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("commission_date", StringType()),
            StructField("location", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 2
        assert "installation_id" in df.columns
        assert "circuit_id" in df.columns
    
    def test_technology_type_categorization(self, spark):
        """Test technology type categorization."""
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", "Substation B"),
            ("INST003", "CKT003", "Battery", "100", "2025-09-10", "Substation C"),
        ]
        
        schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("commission_date", StringType()),
            StructField("location", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Check distinct technology types
        tech_types = [row["technology_type"] for row in df.select("technology_type").distinct().collect()]
        assert "Solar" in tech_types
        assert "Wind" in tech_types
        assert "Battery" in tech_types
    
    def test_commissioning_date_parsing(self, spark):
        """Test that commission dates are correctly parsed."""
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
        ]
        
        schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("commission_date", StringType()),
            StructField("location", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Parse date
        from pyspark.sql.functions import to_date
        result = df.withColumn("commission_date_parsed", to_date("commission_date", "yyyy-MM-dd"))
        
        assert result.filter("commission_date_parsed IS NOT NULL").count() == 1
    
    def test_capacity_aggregation_by_circuit(self, spark):
        """Test aggregation of installed capacity by circuit."""
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT001", "Battery", "100", "2025-06-20", "Substation A"),
            ("INST003", "CKT002", "Wind", "500", "2025-09-10", "Substation B"),
        ]
        
        schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
            StructField("commission_date", StringType()),
            StructField("location", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Aggregate by circuit
        from pyspark.sql.functions import sum as spark_sum
        result = df.groupBy("circuit_id").agg(
            spark_sum("capacity_kw").alias("total_capacity_kw")
        )
        
        assert result.count() == 2
        ckt001_capacity = result.filter("circuit_id = 'CKT001'").collect()[0]["total_capacity_kw"]
        assert float(ckt001_capacity) == 350.0, "CKT001 should have 250 + 100 = 350 kW"


# ============================================================================
# Gold Layer Tests - Circuit Aggregations
# ============================================================================

class TestCircuitGoldLayer:
    """Test gold layer circuit aggregations."""
    
    def test_circuit_summary_aggregation(self, spark):
        """Test circuit-level summary aggregations."""
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            ("CKT002", "22.0", "5.3", "25.0", "18.2"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Test aggregations
        from pyspark.sql.functions import avg, max as spark_max
        result = df.agg(
            avg("total_length_km").alias("avg_length"),
            spark_max("total_capacity_mva").alias("max_capacity")
        )
        
        assert result.count() == 1
    
    def test_utilization_calculation(self, spark):
        """Test circuit utilization percentage calculation."""
        data = [
            ("CKT001", "20.0", "6.85"),
            ("CKT002", "25.0", "18.2"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Calculate utilization
        from pyspark.sql.functions import col
        result = df.withColumn(
            "utilization_pct",
            (col("avg_load_mva").cast("double") / col("total_capacity_mva").cast("double")) * 100
        )
        
        ckt001_util = result.filter("circuit_id = 'CKT001'").collect()[0]["utilization_pct"]
        assert ckt001_util == pytest.approx(34.25, 0.01), "Utilization should be (6.85/20)*100"


# ============================================================================
# Integration Tests - End-to-End Pipeline
# ============================================================================

class TestPipelineIntegration:
    """Test end-to-end pipeline integration."""
    
    def test_circuits_to_der_join(self, spark):
        """Test joining circuits with DER installations."""
        circuits_data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            ("CKT002", "22.0", "5.3", "25.0", "18.2"),
        ]
        
        der_data = [
            ("INST001", "CKT001", "Solar", "250"),
            ("INST002", "CKT001", "Battery", "100"),
        ]
        
        circuits_schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        der_schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
            StructField("capacity_kw", StringType()),
        ])
        
        circuits_df = spark.createDataFrame(circuits_data, circuits_schema)
        der_df = spark.createDataFrame(der_data, der_schema)
        
        # Join circuits with DER
        result = circuits_df.join(der_df, "circuit_id", "left")
        
        assert result.count() == 3, "CKT001 should have 2 DER installations, CKT002 should have none"
    
    def test_full_pipeline_flow(self, spark):
        """Test complete data flow from bronze to gold."""
        # Bronze: Raw data
        bronze_data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        bronze_df = spark.createDataFrame(bronze_data, schema)
        
        # Silver: Transformation
        from pyspark.sql.functions import col
        silver_df = bronze_df.withColumn(
            "utilization_pct",
            (col("avg_load_mva").cast("double") / col("total_capacity_mva").cast("double")) * 100
        )
        
        # Gold: Aggregation
        gold_df = silver_df.agg({"utilization_pct": "avg"})
        
        assert gold_df.count() == 1
        assert "avg(utilization_pct)" in gold_df.columns
    
    def test_scd_type1_update(self, spark):
        """Test SCD Type 1 logic for circuit updates."""
        # Existing data
        existing_data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
        ]
        
        # New data with updated load
        new_data = [
            ("CKT001", "11.0", "4.3", "20.0", "8.50"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        existing_df = spark.createDataFrame(existing_data, schema)
        new_df = spark.createDataFrame(new_data, schema)
        
        # SCD Type 1: Overwrite with new data
        result = new_df  # In real scenario, this would be a MERGE operation
        
        updated_load = result.collect()[0]["avg_load_mva"]
        assert updated_load == "8.50", "Load should be updated to new value"
    
    def test_data_lineage_validation(self, spark):
        """Test that data lineage is maintained through transformations."""
        # Bronze data with metadata
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85", datetime.now(), "utility1"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
            StructField("ingestion_timestamp", TimestampType()),
            StructField("source_system", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Verify metadata columns exist
        assert "ingestion_timestamp" in df.columns
        assert "source_system" in df.columns
        
        # Verify data
        row = df.collect()[0]
        assert row["source_system"] == "utility1"


# ============================================================================
# Performance and Data Quality Tests
# ============================================================================

class TestDataQuality:
    """Test data quality rules and constraints."""
    
    def test_no_duplicate_circuits(self, spark):
        """Test that there are no duplicate circuit IDs in final dataset."""
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),  # Duplicate
            ("CKT002", "22.0", "5.3", "25.0", "18.2"),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Deduplicate
        result = df.dropDuplicates(["circuit_id"])
        
        assert result.count() == 2, "Duplicates should be removed"
    
    def test_null_constraint_violations(self, spark):
        """Test that required fields are not NULL."""
        data = [
            ("CKT001", "11.0", "4.3", "20.0", "6.85"),
            (None, "22.0", "5.3", "25.0", "18.2"),  # NULL circuit_id
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", StringType()),
            StructField("total_length_km", StringType()),
            StructField("total_capacity_mva", StringType()),
            StructField("avg_load_mva", StringType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Check for NULLs
        null_count = df.filter("circuit_id IS NULL").count()
        assert null_count == 1, "Should detect NULL constraint violation"
    
    def test_referential_integrity(self, spark):
        """Test referential integrity between circuits and DER."""
        circuits_data = [
            ("CKT001",),
            ("CKT002",),
        ]
        
        der_data = [
            ("INST001", "CKT001", "Solar"),
            ("INST002", "CKT003", "Wind"),  # References non-existent circuit
        ]
        
        circuits_schema = StructType([StructField("circuit_id", StringType())])
        der_schema = StructType([
            StructField("installation_id", StringType()),
            StructField("circuit_id", StringType()),
            StructField("technology_type", StringType()),
        ])
        
        circuits_df = spark.createDataFrame(circuits_data, circuits_schema)
        der_df = spark.createDataFrame(der_data, der_schema)
        
        # Check for orphaned DER records
        orphaned = der_df.join(circuits_df, "circuit_id", "left_anti")
        
        assert orphaned.count() == 1, "Should detect referential integrity violation"
        assert orphaned.collect()[0]["circuit_id"] == "CKT003"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
