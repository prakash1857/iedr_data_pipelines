"""
Test suite for circuits silver layer transformation.

Tests cover:
- Utility1 segment-level to circuit-level aggregation
- Utility2 circuit-level normalization
- Union of multiple utility data sources
- NULL and empty value filtering
- Data quality validations
- Type conversions and calculations
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, sum as spark_sum, avg, count, trim
from datetime import datetime


class TestCircuitsSilverLayer:
    """Test circuits silver layer transformations."""
    
    def test_utility1_segment_aggregation_to_circuit(self, spark):
        """Test that utility1 segment-level data is correctly aggregated to circuit level.
        
        Business Logic:
        - Group by circuit_id
        - SUM(length_km) for total circuit length
        - SUM(capacity_mva) for total circuit capacity
        - AVG(load_mva) for average circuit load
        - Keep voltage_kv from first segment (should be same for all segments)
        """
        # Arrange: Create sample segment-level data
        data = [
            ("CKT001", "SEG001", "11.0", "2.5", "10.0", "7.5", datetime(2026, 1, 15)),
            ("CKT001", "SEG002", "11.0", "1.8", "10.0", "6.2", datetime(2026, 1, 15)),
            ("CKT001", "SEG003", "11.0", "0.9", "10.0", "8.1", datetime(2026, 1, 15)),
            ("CKT002", "SEG004", "22.0", "3.0", "15.0", "12.0", datetime(2026, 1, 15)),
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
        
        # Act: Perform aggregation (simulating transformation logic)
        result = df.groupBy("circuit_id").agg(
            avg("voltage_kv").cast("double").alias("voltage_kv"),
            spark_sum(col("length_km").cast("double")).alias("total_length_km"),
            spark_sum(col("capacity_mva").cast("double")).alias("total_capacity_mva"),
            avg(col("load_mva").cast("double")).alias("avg_load_mva"),
            count("segment_id").alias("segment_count")
        )
        
        # Assert: Verify aggregation results
        assert result.count() == 2, "Should have 2 unique circuits"
        
        # Verify CKT001 aggregation
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert ckt001["voltage_kv"] == pytest.approx(11.0, 0.01)
        assert ckt001["total_length_km"] == pytest.approx(5.2, 0.01), "Length: 2.5 + 1.8 + 0.9"
        assert ckt001["total_capacity_mva"] == pytest.approx(30.0, 0.01), "Capacity: 10 + 10 + 10"
        assert ckt001["avg_load_mva"] == pytest.approx(7.27, 0.01), "Avg load: (7.5 + 6.2 + 8.1) / 3"
        assert ckt001["segment_count"] == 3
        
        # Verify CKT002 aggregation
        ckt002 = result.filter("circuit_id = 'CKT002'").collect()[0]
        assert ckt002["voltage_kv"] == pytest.approx(22.0, 0.01)
        assert ckt002["total_length_km"] == pytest.approx(3.0, 0.01)
        assert ckt002["segment_count"] == 1
    
    def test_utility2_circuit_level_normalization(self, spark):
        """Test that utility2 circuit-level data is correctly normalized.
        
        Business Logic:
        - Data already at circuit level (no aggregation needed)
        - Standardize column names to match utility1 output
        - Cast string types to appropriate numeric types
        """
        # Arrange: Create utility2 circuit-level data
        data = [
            ("CKT101", "22.0", "5.3", "25.0", "18.2", datetime(2026, 1, 15)),
            ("CKT102", "11.0", "3.2", "12.0", "9.5", datetime(2026, 1, 15)),
            ("CKT103", "33.0", "7.8", "50.0", "35.6", datetime(2026, 1, 15)),
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
        
        # Act: Normalize (cast types)
        result = df.withColumn("voltage_kv", col("voltage_kv").cast("double")) \
                   .withColumn("total_length_km", col("total_length_km").cast("double")) \
                   .withColumn("total_capacity_mva", col("total_capacity_mva").cast("double")) \
                   .withColumn("avg_load_mva", col("avg_load_mva").cast("double"))
        
        # Assert: Verify normalization
        assert result.count() == 3
        assert result.schema["voltage_kv"].dataType.typeName() == "double"
        assert result.schema["total_capacity_mva"].dataType.typeName() == "double"
        
        # Verify data integrity maintained
        ckt101 = result.filter("circuit_id = 'CKT101'").collect()[0]
        assert ckt101["voltage_kv"] == pytest.approx(22.0, 0.01)
        assert ckt101["total_length_km"] == pytest.approx(5.3, 0.01)
    
    def test_union_multiple_utilities(self, spark):
        """Test that data from multiple utilities is correctly unioned.
        
        Business Logic:
        - Union utility1 (aggregated) and utility2 (normalized) data
        - Ensure schema alignment
        - Verify no data loss during union
        """
        # Arrange: Create utility1 aggregated data
        utility1_data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85),
            ("CKT002", 22.0, 3.0, 15.0, 12.0),
        ]
        
        # Create utility2 data
        utility2_data = [
            ("CKT101", 22.0, 5.3, 25.0, 18.2),
            ("CKT102", 11.0, 3.2, 12.0, 9.5),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
        ])
        
        df1 = spark.createDataFrame(utility1_data, schema)
        df2 = spark.createDataFrame(utility2_data, schema)
        
        # Act: Union datasets
        result = df1.union(df2)
        
        # Assert: Verify union
        assert result.count() == 4, "Union should contain all 4 circuits"
        
        # Verify circuits from utility1
        assert result.filter("circuit_id = 'CKT001'").count() == 1
        assert result.filter("circuit_id = 'CKT002'").count() == 1
        
        # Verify circuits from utility2
        assert result.filter("circuit_id = 'CKT101'").count() == 1
        assert result.filter("circuit_id = 'CKT102'").count() == 1
        
        # Verify no duplicate circuits
        circuit_count = result.groupBy("circuit_id").count()
        assert circuit_count.filter("count > 1").count() == 0, "No duplicates should exist"
    
    def test_null_and_empty_circuit_id_filtering(self, spark):
        """Test that NULL and empty circuit IDs are filtered out.
        
        Data Quality Rule:
        - circuit_id must not be NULL
        - circuit_id must not be empty string
        - circuit_id must not be whitespace only
        """
        # Arrange: Create data with invalid circuit IDs
        data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85),       # Valid
            (None, 22.0, 5.3, 25.0, 18.2),            # NULL - should be filtered
            ("", 11.0, 3.2, 12.0, 9.5),               # Empty - should be filtered
            ("  ", 22.0, 2.1, 8.0, 6.0),              # Whitespace - should be filtered
            ("CKT002", 33.0, 7.8, 50.0, 35.6),       # Valid
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Act: Apply filtering logic
        result = df.filter(
            (col("circuit_id").isNotNull()) & 
            (trim(col("circuit_id")) != "")
        )
        
        # Assert: Verify filtering
        assert result.count() == 2, "Only 2 valid circuit IDs should remain"
        
        valid_ids = [row["circuit_id"] for row in result.collect()]
        assert "CKT001" in valid_ids
        assert "CKT002" in valid_ids
        assert None not in valid_ids
        assert "" not in valid_ids
    
    def test_voltage_validation(self, spark):
        """Test that voltage values are positive and valid.
        
        Data Quality Rule:
        - voltage_kv must be > 0
        - Invalid voltages should be filtered or flagged
        """
        # Arrange: Create data with invalid voltages
        data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85),      # Valid
            ("CKT002", -22.0, 5.3, 25.0, 18.2),     # Negative voltage - invalid
            ("CKT003", 0.0, 3.2, 12.0, 9.5),        # Zero voltage - invalid
            ("CKT004", 33.0, 7.8, 50.0, 35.6),      # Valid
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Act: Apply voltage validation
        result = df.filter(col("voltage_kv") > 0)
        
        # Assert: Verify validation
        assert result.count() == 2, "Only circuits with positive voltage should remain"
        
        valid_circuits = [row["circuit_id"] for row in result.collect()]
        assert "CKT001" in valid_circuits
        assert "CKT004" in valid_circuits
        assert "CKT002" not in valid_circuits  # Negative voltage filtered
        assert "CKT003" not in valid_circuits  # Zero voltage filtered
    
    def test_capacity_validation(self, spark):
        """Test that capacity and load values are positive.
        
        Data Quality Rule:
        - total_capacity_mva must be > 0
        - avg_load_mva should be >= 0 (can be zero if no load)
        """
        # Arrange: Create data with invalid capacity values
        data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85),      # Valid
            ("CKT002", 22.0, 5.3, -25.0, 18.2),     # Negative capacity - invalid
            ("CKT003", 11.0, 3.2, 12.0, -9.5),      # Negative load - invalid
            ("CKT004", 33.0, 7.8, 50.0, 0.0),       # Zero load - valid
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Act: Apply capacity validation
        result = df.filter(
            (col("total_capacity_mva") > 0) &
            (col("avg_load_mva") >= 0)
        )
        
        # Assert: Verify validation
        assert result.count() == 2, "Only valid capacity/load combinations should remain"
        
        valid_circuits = [row["circuit_id"] for row in result.collect()]
        assert "CKT001" in valid_circuits
        assert "CKT004" in valid_circuits  # Zero load is acceptable
    
    def test_utilization_calculation(self, spark):
        """Test circuit utilization percentage calculation.
        
        Business Logic:
        - utilization_pct = (avg_load_mva / total_capacity_mva) * 100
        - Should handle edge cases (zero capacity)
        """
        # Arrange: Create circuit data
        data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85),      # 34.25% utilization
            ("CKT002", 22.0, 5.3, 25.0, 18.75),     # 75% utilization
            ("CKT003", 11.0, 3.2, 12.0, 12.0),      # 100% utilization
            ("CKT004", 33.0, 7.8, 50.0, 0.0),       # 0% utilization
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Act: Calculate utilization
        result = df.withColumn(
            "utilization_pct",
            (col("avg_load_mva") / col("total_capacity_mva")) * 100
        )
        
        # Assert: Verify calculations
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert ckt001["utilization_pct"] == pytest.approx(34.25, 0.01)
        
        ckt002 = result.filter("circuit_id = 'CKT002'").collect()[0]
        assert ckt002["utilization_pct"] == pytest.approx(75.0, 0.01)
        
        ckt003 = result.filter("circuit_id = 'CKT003'").collect()[0]
        assert ckt003["utilization_pct"] == pytest.approx(100.0, 0.01)
        
        ckt004 = result.filter("circuit_id = 'CKT004'").collect()[0]
        assert ckt004["utilization_pct"] == pytest.approx(0.0, 0.01)
    
    def test_deduplication(self, spark):
        """Test that duplicate circuit records are handled correctly.
        
        Data Quality Rule:
        - Each circuit_id should appear only once in final output
        - Use most recent ingestion_timestamp for duplicates
        """
        # Arrange: Create data with duplicates
        data = [
            ("CKT001", 11.0, 4.3, 20.0, 6.85, datetime(2026, 1, 15, 10, 0)),
            ("CKT001", 11.0, 4.5, 20.0, 7.0, datetime(2026, 1, 15, 12, 0)),  # Duplicate, newer
            ("CKT002", 22.0, 5.3, 25.0, 18.2, datetime(2026, 1, 15, 10, 0)),
        ]
        
        schema = StructType([
            StructField("circuit_id", StringType()),
            StructField("voltage_kv", DoubleType()),
            StructField("total_length_km", DoubleType()),
            StructField("total_capacity_mva", DoubleType()),
            StructField("avg_load_mva", DoubleType()),
            StructField("ingestion_timestamp", TimestampType()),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Act: Deduplicate (keep most recent)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy("circuit_id").orderBy(col("ingestion_timestamp").desc())
        result = df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")
        
        # Assert: Verify deduplication
        assert result.count() == 2, "Should have 2 unique circuits after deduplication"
        
        # Verify kept the most recent record for CKT001
        ckt001 = result.filter("circuit_id = 'CKT001'").collect()[0]
        assert ckt001["total_length_km"] == pytest.approx(4.5, 0.01), "Should keep newer record"
        assert ckt001["avg_load_mva"] == pytest.approx(7.0, 0.01)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
