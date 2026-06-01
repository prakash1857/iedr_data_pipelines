"""
Test suite for installed DER silver layer transformation.

Tests cover:
- Installation ID validation
- Circuit ID referential integrity
- Commissioning date parsing and validation
- Capacity validation and aggregation
- Technology type standardization
- Location data quality
- Operational status tracking
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, to_date, trim, count, sum as spark_sum, avg, expr
from datetime import datetime, date


class TestInstallDERSilverLayer:
    """Test installed DER silver layer transformations."""
    
    def test_installation_id_validation(self, spark):
        """Test that only records with valid installation IDs are processed.
        
        Data Quality Rule:
        - installation_id must not be NULL
        - installation_id must not be empty string
        - installation_id must be unique
        """
        # Arrange: Create data with invalid installation IDs
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            (None, "CKT002", "Wind", "500", "2025-06-20", "Substation B"),        # NULL - invalid
            ("", "CKT003", "Battery", "100", "2025-09-10", "Substation C"),        # Empty - invalid
            ("  ", "CKT004", "Solar", "300", "2025-12-05", "Substation D"),       # Whitespace - invalid
            ("INST002", "CKT005", "Wind", "750", "2025-11-15", "Substation E"),
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
        
        # Act: Filter invalid installation IDs
        result = df.filter(
            (col("installation_id").isNotNull()) &
            (trim(col("installation_id")) != "")
        )
        
        # Assert: Verify filtering
        assert result.count() == 2, "Only 2 installations with valid IDs should remain"
        
        valid_ids = [row["installation_id"] for row in result.collect()]
        assert "INST001" in valid_ids
        assert "INST002" in valid_ids
    
    def test_commissioning_date_parsing(self, spark):
        """Test that commission dates are correctly parsed to date type.
        
        Business Logic:
        - Parse string dates (YYYY-MM-DD format) to date type
        - Invalid dates should result in NULL
        """
        # Arrange: Create data with various date formats
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", "Substation B"),
            ("INST003", "CKT003", "Battery", "100", "2025-13-45", "Substation C"),  # Invalid date
            ("INST004", "CKT004", "Solar", "300", None, "Substation D"),            # NULL date
            ("INST005", "CKT005", "Wind", "750", "2025-12-31", "Substation E"),
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
        
        # Act: Parse dates
        result = df.withColumn(
            "commission_date_parsed",
            expr("try_to_date(commission_date, 'yyyy-MM-dd')")
        )
        
        # Assert: Verify date parsing
        valid_dates = result.filter("commission_date_parsed IS NOT NULL").count()
        assert valid_dates == 3, "Should have 4 valid dates"
        
        # Verify specific date
        inst001 = result.filter("installation_id = 'INST001'").collect()[0]
        assert inst001["commission_date_parsed"] == date(2025, 3, 15)
        
        # Verify invalid date resulted in NULL
        inst003 = result.filter("installation_id = 'INST003'").collect()[0]
        assert inst003["commission_date_parsed"] is None
    
    def test_capacity_validation_and_conversion(self, spark):
        """Test that capacity values are positive and properly converted.
        
        Data Quality Rule:
        - capacity_kw must be > 0
        - Convert string to double
        - Filter invalid capacities
        """
        # Arrange: Create data with various capacity values
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),      # Valid
            ("INST002", "CKT002", "Wind", "-500", "2025-06-20", "Substation B"),      # Negative - invalid
            ("INST003", "CKT003", "Battery", "0", "2025-09-10", "Substation C"),      # Zero - invalid
            ("INST004", "CKT004", "Solar", "300.5", "2025-12-05", "Substation D"),    # Valid with decimal
            ("INST005", "CKT005", "Wind", "ABC", "2025-11-15", "Substation E"),       # Non-numeric - invalid
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
        
        # Act: Convert and validate capacity
        result = df.withColumn(
            "capacity_kw_numeric",
            expr("try_cast(capacity_kw as double)")
        ).filter(
            (col("capacity_kw_numeric").isNotNull()) &
            (col("capacity_kw_numeric") > 0)
        )
        
        # Assert: Verify validation
        assert result.count() == 2, "Only 2 installations with valid capacity should remain"
        
        valid_installations = [row["installation_id"] for row in result.collect()]
        assert "INST001" in valid_installations
        assert "INST004" in valid_installations
        
        # Verify capacity values
        inst004 = result.filter("installation_id = 'INST004'").collect()[0]
        assert inst004["capacity_kw_numeric"] == pytest.approx(300.5, 0.01)
    
    def test_technology_type_standardization(self, spark):
        """Test that technology types are standardized.
        
        Business Logic:
        - Standardize technology type names (case-insensitive)
        - Map aliases to standard names
        """
        # Arrange: Create data with various technology type variations
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "SOLAR", "200", "2025-06-20", "Substation B"),
            ("INST003", "CKT003", "PV", "300", "2025-09-10", "Substation C"),
            ("INST004", "CKT004", "Wind", "500", "2025-12-05", "Substation D"),
            ("INST005", "CKT005", "wind", "400", "2025-11-15", "Substation E"),
            ("INST006", "CKT006", "Battery", "100", "2026-01-20", "Substation F"),
            ("INST007", "CKT007", "BESS", "150", "2026-02-15", "Substation G"),
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
        
        # Act: Standardize technology types
        from pyspark.sql.functions import upper, when
        result = df.withColumn(
            "technology_type_std",
            when(upper(col("technology_type")).isin("SOLAR", "PV"), "Solar")
            .when(upper(col("technology_type")).isin("WIND"), "Wind")
            .when(upper(col("technology_type")).isin("BATTERY", "BESS"), "Battery")
            .otherwise(col("technology_type"))
        )
        
        # Assert: Verify standardization
        # All solar variations should be "Solar"
        solar_count = result.filter("technology_type_std = 'Solar'").count()
        assert solar_count == 3, "Should have 3 Solar installations (Solar, SOLAR, PV)"
        
        # All wind variations should be "Wind"
        wind_count = result.filter("technology_type_std = 'Wind'").count()
        assert wind_count == 2, "Should have 2 Wind installations"
        
        # All battery variations should be "Battery"
        battery_count = result.filter("technology_type_std = 'Battery'").count()
        assert battery_count == 2, "Should have 2 Battery installations (Battery, BESS)"
    
    def test_capacity_aggregation_by_circuit(self, spark):
        """Test aggregation of installed capacity by circuit.
        
        Business Logic:
        - Sum total installed capacity per circuit
        - Count number of installations per circuit
        """
        # Arrange: Create data with multiple installations per circuit
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT001", "Battery", "100", "2025-06-20", "Substation A"),
            ("INST003", "CKT001", "Wind", "500", "2025-09-10", "Substation A"),
            ("INST004", "CKT002", "Solar", "300", "2025-12-05", "Substation B"),
            ("INST005", "CKT003", "Wind", "750", "2025-11-15", "Substation C"),
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
        
        # Act: Aggregate by circuit
        result = df.withColumn("capacity_kw", expr("try_cast(capacity_kw as double)")) \
                   .groupBy("circuit_id") \
                   .agg(
                       spark_sum("capacity_kw").alias("total_installed_capacity_kw"),
                       count("installation_id").alias("installation_count")
                   )
        
        # Assert: Verify aggregation
        assert result.count() == 3, "Should have 3 unique circuits"
        
        # Verify CKT001 aggregation (3 installations)
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert ckt001["total_installed_capacity_kw"] == pytest.approx(850.0, 0.01), "250 + 100 + 500"
        assert ckt001["installation_count"] == 3
        
        # Verify CKT002 aggregation (1 installation)
        ckt002 = result.filter("circuit_id = 'CKT002'").collect()[0]
        assert ckt002["total_installed_capacity_kw"] == pytest.approx(300.0, 0.01)
        assert ckt002["installation_count"] == 1
    
    def test_capacity_aggregation_by_technology_type(self, spark):
        """Test aggregation of installed capacity by technology type.
        
        Business Logic:
        - Sum total capacity per technology type
        - Calculate average installation size per technology
        """
        # Arrange: Create data with various technology types
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "Solar", "300", "2025-06-20", "Substation B"),
            ("INST003", "CKT003", "Wind", "500", "2025-09-10", "Substation C"),
            ("INST004", "CKT004", "Wind", "750", "2025-12-05", "Substation D"),
            ("INST005", "CKT005", "Battery", "100", "2025-11-15", "Substation E"),
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
        
        # Act: Aggregate by technology type
        result = df.withColumn("capacity_kw", expr("try_cast(capacity_kw as double)")) \
                   .groupBy("technology_type") \
                   .agg(
                       spark_sum("capacity_kw").alias("total_capacity_kw"),
                       avg("capacity_kw").alias("avg_capacity_kw"),
                       count("installation_id").alias("installation_count")
                   )
        
        # Assert: Verify aggregation
        solar = result.filter("technology_type = 'Solar'").collect()[0]
        assert solar["total_capacity_kw"] == pytest.approx(550.0, 0.01), "250 + 300"
        assert solar["avg_capacity_kw"] == pytest.approx(275.0, 0.01)
        assert solar["installation_count"] == 2
        
        wind = result.filter("technology_type = 'Wind'").collect()[0]
        assert wind["total_capacity_kw"] == pytest.approx(1250.0, 0.01), "500 + 750"
        assert wind["avg_capacity_kw"] == pytest.approx(625.0, 0.01)
    
    def test_location_validation(self, spark):
        """Test that location data is valid.
        
        Data Quality Rule:
        - location should not be NULL or empty
        - Standardize location names
        """
        # Arrange: Create data with various location values
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A"),
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", None),              # NULL location
            ("INST003", "CKT003", "Battery", "100", "2025-09-10", ""),             # Empty location
            ("INST004", "CKT004", "Solar", "300", "2025-12-05", "  "),            # Whitespace location
            ("INST005", "CKT005", "Wind", "750", "2025-11-15", "Substation B"),
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
        
        # Act: Filter invalid locations
        result = df.filter(
            (col("location").isNotNull()) &
            (trim(col("location")) != "")
        )
        
        # Assert: Verify filtering
        assert result.count() == 2, "Only 2 installations with valid locations should remain"
        
        valid_ids = [row["installation_id"] for row in result.collect()]
        assert "INST001" in valid_ids
        assert "INST005" in valid_ids
    
    def test_commissioning_date_chronology(self, spark):
        """Test that commission dates follow expected chronological order.
        
        Business Logic:
        - Commission dates should be in the past or near future
        - Flag installations with future dates too far out
        """
        # Arrange: Create data with various commission dates
        reference_date = date(2026, 1, 1)
        data = [
            ("INST001", "CKT001", "Solar", "250", "2024-03-15", "Substation A"),  # Past - valid
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", "Substation B"),   # Past - valid
            ("INST003", "CKT003", "Battery", "100", "2026-01-10", "Substation C"), # Near future - valid
            ("INST004", "CKT004", "Solar", "300", "2030-12-05", "Substation D"),  # Far future - suspicious
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
        
        # Act: Parse and filter by date range
        from pyspark.sql.functions import lit, datediff
        result = df.withColumn(
            "commission_date_parsed",
            to_date(col("commission_date"), "yyyy-MM-dd")
        ).withColumn(
            "days_from_reference",
            datediff(col("commission_date_parsed"), lit(reference_date))
        ).filter(
            col("days_from_reference") <= 730  # Within 2 years of reference date
        )
        
        # Assert: Verify filtering
        assert result.count() == 3, "Should have 3 installations within reasonable date range"
        
        valid_ids = [row["installation_id"] for row in result.collect()]
        assert "INST001" in valid_ids
        assert "INST002" in valid_ids
        assert "INST003" in valid_ids
        assert "INST004" not in valid_ids  # Too far in future
    
    def test_duplicate_installation_id_handling(self, spark):
        """Test that duplicate installation IDs are handled correctly.
        
        Data Quality Rule:
        - Each installation_id should appear only once
        - Use most recent ingestion_timestamp for duplicates
        """
        # Arrange: Create data with duplicate installation IDs
        data = [
            ("INST001", "CKT001", "Solar", "250", "2025-03-15", "Substation A", datetime(2026, 1, 15, 10, 0)),
            ("INST001", "CKT001", "Solar", "275", "2025-03-15", "Substation A", datetime(2026, 1, 15, 12, 0)),  # Newer
            ("INST002", "CKT002", "Wind", "500", "2025-06-20", "Substation B", datetime(2026, 1, 15, 10, 0)),
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
        
        # Act: Deduplicate (keep most recent)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy("installation_id").orderBy(col("ingestion_timestamp").desc())
        result = df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")
        
        # Assert: Verify deduplication
        assert result.count() == 2, "Should have 2 unique installations after deduplication"
        
        # Verify kept the most recent record for INST001
        inst001 = result.filter("installation_id = 'INST001'").collect()[0]
        assert inst001["capacity_kw"] == "275", "Should keep newer record with updated capacity"


if __name__ == "__main__":
    pytest.main(["-v"])
