# IEDR Pipeline Test Suite

Comprehensive test coverage for the IEDR (Integrated Electricity Distribution Resource) data pipeline.

## Overview

This test suite validates the IEDR Pipeline across all layers (Bronze, Silver, Gold) and includes:
- **37 unit tests** covering individual transformations
- **Integration tests** validating end-to-end data flow
- **Data quality tests** ensuring referential integrity and constraints
- **Schema validation** for all data layers

## Test Structure

```
tests/
├── conftest.py                  # Pytest configuration and fixtures
├── iedr_pipeline_test.py         # Main test suite (renamed to iedr_pipeline_test.py recommended)
└── README.md                    # This file

fixtures/
├── utility1_circuits_sample.json
├── utility2_circuits_sample.json
├── planned_der_sample.json
└── install_der_sample.json
```

## Test Coverage

### Bronze Layer Tests (4 tests)
- ✅ Utility1 circuits schema validation (segment-level data)
- ✅ Utility2 circuits schema validation (circuit-level data)
- ✅ Planned DER schema validation
- ✅ Installed DER schema validation

### Silver Layer - Circuits (6 tests)
- ✅ Utility1 segment aggregation to circuit level
- ✅ Utility2 normalization
- ✅ Union operation between utilities
- ✅ NULL and empty string filtering
- ✅ Data quality checks (positive voltages, capacities)
- ✅ Schema consistency across sources

### Silver Layer - Planned DER (4 tests)
- ✅ Circuit ID validation
- ✅ Date parsing for commission dates
- ✅ Project status filtering
- ✅ Capacity validation (positive values)

### Silver Layer - Installed DER (4 tests)
- ✅ Installation data processing
- ✅ Technology type categorization (Solar, Wind, Battery)
- ✅ Commissioning date parsing
- ✅ Capacity aggregation by circuit

### Gold Layer - Circuit Aggregations (2 tests)
- ✅ Circuit summary aggregations
- ✅ Utilization percentage calculation

### Integration Tests (5 tests)
- ✅ Circuits to DER join operations
- ✅ Full pipeline flow (Bronze → Silver → Gold)
- ✅ SCD Type 1 update logic
- ✅ Data lineage validation
- ✅ Metadata preservation

### Data Quality Tests (3 tests)
- ✅ Duplicate circuit detection and removal
- ✅ NULL constraint violation detection
- ✅ Referential integrity (circuits ↔ DER)

## Running the Tests

### Option 1: Using Databricks Asset Bundles (Recommended)

```bash
# From your local development environment with databricks-connect
cd /path/to/iedr_data_pipelines/iedr_project
databricks bundle test
```

This is the **recommended approach** as it:
- Runs tests in isolation
- Properly handles Spark session management
- Integrates with CI/CD pipelines
- Avoids Databricks workspace filesystem limitations

### Option 2: Local Development with pytest

```bash
# Ensure databricks-connect is configured
databricks auth login

# Install test dependencies
pip install pytest databricks-connect databricks-sdk

# Run all tests
pytest tests/iedr_pipeline_test.py -v

# Run specific test class
pytest tests/iedr_pipeline_test.py::TestCircuitsSilverLayer -v

# Run specific test
pytest tests/iedr_pipeline_test.py::TestCircuitsSilverLayer::test_utility1_segment_aggregation -v

# Run with coverage
pytest tests/ --cov=src/pipelines/IEDR_Pipeline --cov-report=html
```

### Option 3: Notebook-Based Testing (Limited)

⚠️ **Note**: Direct pytest execution in Databricks notebooks has limitations due to:
- Workspace filesystem constraints (`__pycache__` creation issues)
- Module import conflicts
- Pytest's file system requirements

If you need to run tests in a notebook, use the custom test runner approach:

1. Navigate to: `/Workspace/Users/prakash1857@gmail.com/iedr_data_pipelines/iedr_project/src/pipelines/IEDR_Pipeline/explorations/run_silver_layer_tests`
2. Run the notebook cells sequentially
3. The notebook imports test functions and runs them without pytest framework

## Test Fixtures

Test fixtures are stored as JSON files in the `fixtures/` directory:

### utility1_circuits_sample.json
```json
[
  {
    "circuit_id": "CKT001",
    "segment_id": "SEG001",
    "voltage_kv": "11.0",
    "length_km": "2.5",
    "capacity_mva": "10.0",
    "load_mva": "7.5"
  }
]
```

### utility2_circuits_sample.json
```json
[
  {
    "circuit_id": "CKT101",
    "voltage_kv": "22.0",
    "total_length_km": "5.3",
    "total_capacity_mva": "25.0",
    "avg_load_mva": "18.2"
  }
]
```

## Pipeline Configuration

The tests validate data according to the IEDR Pipeline configuration:

```yaml
Pipeline ID: 61758a1d-1c7a-4842-9817-e99191b5f4dd
Catalog: iedr
Schema: silver
Target Tables:
  - circuits
  - circuits_scd (SCD Type 1)
  - planned_der
  - install_der
  - circuit (gold layer)
  - circuit_der (gold layer)

Configuration:
  - Photon: Enabled
  - Serverless: Enabled
  - Runtime: CURRENT channel
```

## Key Testing Patterns

### 1. Schema Validation
Tests verify that data adheres to expected schemas at each layer:

```python
def test_bronze_schema(spark):
    expected_fields = ["circuit_id", "voltage_kv", ...]
    df = spark.createDataFrame(data, schema)
    for field in expected_fields:
        assert field in df.columns
```

### 2. Transformation Logic
Tests validate business logic transformations:

```python
def test_utility1_aggregation(spark):
    # Input: Segment-level data
    # Expected: Circuit-level aggregation
    result = df.groupBy("circuit_id").agg(...)
    assert result.count() == expected_count
```

### 3. Data Quality Rules
Tests ensure data quality constraints:

```python
def test_null_filtering(spark):
    result = df.filter(col("circuit_id").isNotNull())
    assert result.count() == valid_records_count
```

### 4. Integration Testing
Tests validate cross-layer data flow:

```python
def test_circuits_to_der_join(spark):
    result = circuits_df.join(der_df, "circuit_id")
    assert result.count() == expected_joined_records
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: IEDR Pipeline Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install databricks-connect databricks-sdk pytest
      - name: Configure Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks auth login --host $DATABRICKS_HOST --token $DATABRICKS_TOKEN
      - name: Run tests
        run: |
          cd iedr_data_pipelines/iedr_project
          databricks bundle test
```

## Test Maintenance

### Adding New Tests

1. **Create test function** following pytest naming convention (`test_*`)
2. **Use fixtures** from `conftest.py` (spark, load_fixture)
3. **Follow AAA pattern**: Arrange, Act, Assert
4. **Add docstrings** explaining what is being tested
5. **Update this README** with new test coverage

### Example Test Template

```python
def test_new_transformation(self, spark):
    """Test description: what are we validating?"""
    # Arrange: Set up test data
    data = [(...)]
    schema = StructType([...])
    df = spark.createDataFrame(data, schema)
    
    # Act: Apply transformation
    result = df.transform(my_transformation)
    
    # Assert: Verify expectations
    assert result.count() == expected_count
    assert result.filter("condition").count() == expected_filtered_count
```

## Troubleshooting

### Common Issues

**Issue**: `OSError [Errno 95]: Operation not supported` when running pytest in notebooks

**Solution**: Use `databricks bundle test` from local environment or use the custom notebook test runner.

---

**Issue**: `Import file mismatch` errors

**Solution**: Ensure you're running tests from the project root directory and avoid mixing notebook and local test executions.

---

**Issue**: Spark fixture not found

**Solution**: Ensure `conftest.py` is in the same directory as your test files and uses the correct Spark session initialization.

---

**Issue**: Fixture files not found

**Solution**: Verify `fixtures/` directory is at `iedr_data_pipelines/iedr_project/fixtures/` and contains required JSON files.

## Best Practices

1. **Test Isolation**: Each test should be independent and not rely on other tests
2. **Data Driven**: Use parametrize for testing multiple scenarios
3. **Meaningful Assertions**: Use descriptive assertion messages
4. **Performance**: Keep individual tests fast (<5 seconds)
5. **Coverage**: Aim for >80% code coverage for transformation logic
6. **Documentation**: Keep this README updated with new tests

## Resources

- [Databricks Connect Documentation](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [Databricks Asset Bundles Testing](https://docs.databricks.com/dev-tools/bundles/testing.html)
- [pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)

## Contact

For questions or issues with the test suite, contact the IEDR Pipeline team or file an issue in the project repository.
