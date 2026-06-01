"""
Test suite for planned DER silver layer transformation.

Tests cover:
- Circuit ID validation and referential integrity
- Date parsing and validation
- Project status filtering
- Capacity validation and type conversion
- Technology type categorization
- Data quality checks
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, to_date, trim, count, sum as spark_sum, expr
from datetime import datetime, date


class TestPlannedDERSilverLayer:
    """Test planned DER silver layer transformations."""
    
    def test_circuit_id_validation(self, spark):
        """Test that only records with valid circuit IDs are processed.
        
        Data Quality Rule:
        - circuit_id must not be NULL
        - circuit_id must not be empty string
        - circuit_id must not be whitespace only
        """
        # Arrange: Create planned DER data with invalid circuit IDs
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", None, "Wind", "1000", "2026-08-20", "Planned"),           # NULL circuit_id
            ("PRJ003", "", "Battery", "250", "2026-09-10", "Planned"),            # Empty circuit_id
            ("PRJ004", "  ", "Solar", "750", "2026-10-05", "Planned"),           # Whitespace circuit_id
            ("PRJ005", "CKT002", "Wind", "1500", "2026-11-15", "Planned"),
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
        
        # Act: Filter invalid circuit IDs
        result = df.filter(
            (col("circuit_id").isNotNull()) &
            (trim(col("circuit_id")) != "")
        )
        
        # Assert: Verify only valid records remain
        assert result.count() == 2, "Only 2 projects with valid circuit IDs should remain"
        
        valid_projects = [row["project_id"] for row in result.collect()]
        assert "PRJ001" in valid_projects
        assert "PRJ005" in valid_projects
        assert "PRJ002" not in valid_projects  # NULL filtered
        assert "PRJ003" not in valid_projects  # Empty filtered
        assert "PRJ004" not in valid_projects  # Whitespace filtered
    
    def test_date_parsing_and_validation(self, spark):
        """Test that planned commission dates are correctly parsed to date type.
        
        Business Logic:
        - Parse string dates (YYYY-MM-DD format) to date type
        - Invalid dates should be handled (NULL or filtered)
        """
        # Arrange: Create data with various date formats
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),       # Valid date
            ("PRJ002", "CKT002", "Wind", "1000", "2026-08-20", "Planned"),       # Valid date
            ("PRJ003", "CKT003", "Battery", "250", "2026-13-45", "Planned"),     # Invalid date
            ("PRJ004", "CKT004", "Solar", "750", None, "Planned"),               # NULL date
            ("PRJ005", "CKT005", "Wind", "1500", "2026-12-31", "Planned"),       # Valid date
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
        
        # Act: Parse dates
        result = df.withColumn(
            "commission_date",
            expr("try_to_date(planned_commission_date, 'yyyy-MM-dd')")
        )
        
        # Assert: Verify date parsing
        valid_dates = result.filter("commission_date IS NOT NULL").count()
        assert valid_dates == 3, "Should have 3 valid dates"
        
        # Verify specific dates
        prj001 = result.filter("project_id = 'PRJ001'").collect()[0]
        assert prj001["commission_date"] == date(2026, 6, 15)
        
        # Verify invalid dates resulted in NULL
        prj003 = result.filter("project_id = 'PRJ003'").collect()[0]
        assert prj003["commission_date"] is None, "Invalid date should be NULL"
    
    def test_project_status_filtering(self, spark):
        """Test filtering by project status.
        
        Business Logic:
        - Include only 'Planned' or 'Approved' projects
        - Exclude 'Cancelled', 'Completed', or other statuses
        """
        # Arrange: Create projects with various statuses
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT002", "Wind", "1000", "2026-08-20", "Approved"),
            ("PRJ003", "CKT003", "Battery", "250", "2026-09-10", "Cancelled"),
            ("PRJ004", "CKT004", "Solar", "750", "2026-10-05", "Completed"),
            ("PRJ005", "CKT005", "Wind", "1500", "2026-11-15", "On Hold"),
            ("PRJ006", "CKT006", "Solar", "600", "2026-12-20", "Planned"),
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
        
        # Act: Filter active projects only
        result = df.filter(
            col("project_status").isin("Planned", "Approved")
        )
        
        # Assert: Verify filtering
        assert result.count() == 3, "Should have 3 active projects"
        
        statuses = [row["project_status"] for row in result.collect()]
        assert all(status in ["Planned", "Approved"] for status in statuses)
        
        # Verify excluded projects
        project_ids = [row["project_id"] for row in result.collect()]
        assert "PRJ003" not in project_ids  # Cancelled
        assert "PRJ004" not in project_ids  # Completed
        assert "PRJ005" not in project_ids  # On Hold
    
    def test_capacity_validation_and_conversion(self, spark):
        """Test that capacity values are positive and properly converted to numeric.
        
        Data Quality Rule:
        - capacity_kw must be > 0
        - Convert string to double
        - Invalid or negative capacities should be filtered
        """
        # Arrange: Create data with various capacity values
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),       # Valid
            ("PRJ002", "CKT002", "Wind", "-1000", "2026-08-20", "Planned"),      # Negative - invalid
            ("PRJ003", "CKT003", "Battery", "0", "2026-09-10", "Planned"),       # Zero - invalid
            ("PRJ004", "CKT004", "Solar", "750.5", "2026-10-05", "Planned"),     # Valid with decimal
            ("PRJ005", "CKT005", "Wind", "ABC", "2026-11-15", "Planned"),        # Non-numeric - invalid
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
        
        # Act: Convert and validate capacity
        from pyspark.sql.functions import when
        result = df.withColumn(
            "capacity_kw_numeric",
            expr("try_cast(capacity_kw as double)")
        ).filter(
            (col("capacity_kw_numeric").isNotNull()) &
            (col("capacity_kw_numeric") > 0)
        )
        
        # Assert: Verify validation
        assert result.count() == 2, "Only 2 projects with valid positive capacity should remain"
        
        valid_projects = [row["project_id"] for row in result.collect()]
        assert "PRJ001" in valid_projects
        assert "PRJ004" in valid_projects
        
        # Verify capacity values
        prj001 = result.filter("project_id = 'PRJ001'").collect()[0]
        assert prj001["capacity_kw_numeric"] == pytest.approx(500.0, 0.01)
        
        prj004 = result.filter("project_id = 'PRJ004'").collect()[0]
        assert prj004["capacity_kw_numeric"] == pytest.approx(750.5, 0.01)
    
    def test_technology_type_categorization(self, spark):
        """Test that technology types are properly categorized.
        
        Business Logic:
        - Standardize technology type names
        - Valid types: Solar, Wind, Battery, Hydro, Other
        """
        # Arrange: Create data with various technology types
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT002", "SOLAR", "600", "2026-08-20", "Planned"),       # Case variation
            ("PRJ003", "CKT003", "Wind", "1000", "2026-09-10", "Planned"),
            ("PRJ004", "CKT004", "wind", "800", "2026-10-05", "Planned"),        # Case variation
            ("PRJ005", "CKT005", "Battery", "250", "2026-11-15", "Planned"),
            ("PRJ006", "CKT006", "BESS", "300", "2026-12-20", "Planned"),        # Battery alias
            ("PRJ007", "CKT007", "PV", "450", "2027-01-10", "Planned"),          # Solar alias
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
        tech_types = result.select("technology_type_std").distinct().collect()
        tech_type_list = [row["technology_type_std"] for row in tech_types]
        
        assert "Solar" in tech_type_list
        assert "Wind" in tech_type_list
        assert "Battery" in tech_type_list
        
        # Verify specific conversions
        prj002 = result.filter("project_id = 'PRJ002'").collect()[0]
        assert prj002["technology_type_std"] == "Solar", "SOLAR should be standardized to Solar"
        
        prj006 = result.filter("project_id = 'PRJ006'").collect()[0]
        assert prj006["technology_type_std"] == "Battery", "BESS should be standardized to Battery"
        
        prj007 = result.filter("project_id = 'PRJ007'").collect()[0]
        assert prj007["technology_type_std"] == "Solar", "PV should be standardized to Solar"
    
    def test_capacity_aggregation_by_circuit(self, spark):
        """Test aggregation of planned capacity by circuit.
        
        Business Logic:
        - Sum total planned capacity per circuit
        - Count number of projects per circuit
        """
        # Arrange: Create data with multiple projects per circuit
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),
            ("PRJ002", "CKT001", "Battery", "250", "2026-08-20", "Planned"),
            ("PRJ003", "CKT001", "Wind", "1000", "2026-09-10", "Planned"),
            ("PRJ004", "CKT002", "Solar", "750", "2026-10-05", "Planned"),
            ("PRJ005", "CKT003", "Wind", "1500", "2026-11-15", "Planned"),
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
        
        # Act: Aggregate by circuit
        result = df.withColumn("capacity_kw", col("capacity_kw").cast("double")) \
                   .groupBy("circuit_id") \
                   .agg(
                       spark_sum("capacity_kw").alias("total_planned_capacity_kw"),
                       count("project_id").alias("project_count")
                   )
        
        # Assert: Verify aggregation
        assert result.count() == 3, "Should have 3 unique circuits"
        
        # Verify CKT001 aggregation (3 projects)
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert ckt001["total_planned_capacity_kw"] == pytest.approx(1750.0, 0.01), "500 + 250 + 1000"
        assert ckt001["project_count"] == 3
        
        # Verify CKT002 aggregation (1 project)
        ckt002 = result.filter("circuit_id = 'CKT002'").collect()[0]
        assert ckt002["total_planned_capacity_kw"] == pytest.approx(750.0, 0.01)
        assert ckt002["project_count"] == 1
    
    def test_duplicate_project_id_handling(self, spark):
        """Test that duplicate project IDs are handled correctly.
        
        Data Quality Rule:
        - Each project_id should appear only once
        - Use most recent ingestion_timestamp for duplicates
        """
        # Arrange: Create data with duplicate project IDs
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned", datetime(2026, 1, 15, 10, 0)),
            ("PRJ001", "CKT001", "Solar", "550", "2026-06-15", "Planned", datetime(2026, 1, 15, 12, 0)),  # Newer
            ("PRJ002", "CKT002", "Wind", "1000", "2026-08-20", "Planned", datetime(2026, 1, 15, 10, 0)),
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
        
        # Act: Deduplicate (keep most recent)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy("project_id").orderBy(col("ingestion_timestamp").desc())
        result = df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")
        
        # Assert: Verify deduplication
        assert result.count() == 2, "Should have 2 unique projects after deduplication"
        
        # Verify kept the most recent record for PRJ001
        prj001 = result.filter("project_id = 'PRJ001'").collect()[0]
        assert prj001["capacity_kw"] == "550", "Should keep newer record with updated capacity"
    
    def test_commission_date_range_validation(self, spark):
        """Test that commission dates are within valid range.
        
        Business Logic:
        - Commission dates should be in the future (after today)
        - Warn or filter projects with past commission dates
        """
        # Arrange: Create data with various dates
        today = date(2026, 5, 25)
        data = [
            ("PRJ001", "CKT001", "Solar", "500", "2026-06-15", "Planned"),       # Future - valid
            ("PRJ002", "CKT002", "Wind", "1000", "2025-08-20", "Planned"),       # Past - invalid
            ("PRJ003", "CKT003", "Battery", "250", "2027-09-10", "Planned"),     # Far future - valid
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
        
        # Act: Validate date range
        from pyspark.sql.functions import lit
        result = df.withColumn(
            "commission_date",
            to_date(col("planned_commission_date"), "yyyy-MM-dd")
        ).filter(
            col("commission_date") > lit(today)
        )
        
        # Assert: Verify validation
        assert result.count() == 2, "Should have 2 projects with future commission dates"
        
        project_ids = [row["project_id"] for row in result.collect()]
        assert "PRJ001" in project_ids
        assert "PRJ003" in project_ids
        assert "PRJ002" not in project_ids  # Past date filtered


if __name__ == "__main__":
    pytest.main(["-v"])
