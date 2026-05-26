# IEDR Pipeline Test Suite - Silver Layer Tests

Comprehensive test coverage for the IEDR (Integrated Electricity Distribution Resource) data pipeline silver layer transformations.

## Overview

The silver layer test suite validates data quality, business logic, and transformation correctness for:
- **Circuits Silver Layer**: Utility data aggregation and normalization
- **Planned DER Silver Layer**: Project validation and capacity tracking
- **Installed DER Silver Layer**: Installation data processing and aggregation

## Test Structure

```
tests/
├── conftest.py                         # Pytest configuration and fixtures
├── test_circuits_silver_layer.py       # Circuits transformation tests (9 tests)
├── test_planned_der_silver_layer.py    # Planned DER transformation tests (10 tests)
├── test_install_der_silver_layer.py    # Installed DER transformation tests (10 tests)
└── README.md                           # This file

fixtures/
└── .gitkeep                            # Placeholder for test fixtures
```

## Test Coverage Summary

| Test File                            | Tests | Lines | Coverage                                                    |
|--------------------------------------|-------|-------|-------------------------------------------------------------|
| test_circuits_silver_layer.py        | 9     | 375   | Aggregation, union, validation, utilization, deduplication  |
| test_planned_der_silver_layer.py     | 10    | 403   | Validation, dates, status, capacity, technology, aggregation|
| test_install_der_silver_layer.py     | 10    | 422   | Validation, dates, capacity, technology, location, aggregation|
| **Total**                            | **29**| **1200** | **Complete silver layer validation**                     |

## Detailed Test Coverage

### test_circuits_silver_layer.py (9 tests)

#### 1. `test_utility1_segment_aggregation_to_circuit`
**Purpose**: Validate segment-level to circuit-level aggregation for Utility1 data

**Business Logic**:
- Group by `circuit_id`
- `SUM(length_km)` → `total_length_km`
- `SUM(capacity_mva)` → `total_capacity_mva`
- `AVG(load_mva)` → `avg_load_mva`
- `AVG(voltage_kv)` → `voltage_kv` (should be consistent across segments)

**Test Data**: 3 segments for CKT001, 1 segment for CKT002

**Assertions**:
- Circuit count = 2
- CKT001: length = 5.2 km (2.5 + 1.8 + 0.9)
- CKT001: capacity = 30.0 MVA (10 + 10 + 10)
- CKT001: avg_load = 7.27 MVA ((7.5 + 6.2 + 8.1) / 3)

#### 2. `test_utility2_circuit_level_normalization`
**Purpose**: Validate Utility2 data normalization (already at circuit level)

**Business Logic**:
- No aggregation needed (data already circuit-level)
- Cast string types to numeric (double)
- Standardize column names to match Utility1 output

**Test Data**: 3 circuits with string numeric values

**Assertions**:
- Schema has double types for numeric columns
- Data integrity maintained after type conversion

#### 3. `test_union_multiple_utilities`
**Purpose**: Validate union of Utility1 and Utility2 datasets

**Business Logic**:
- Union aggregated Utility1 data with normalized Utility2 data
- Ensure schema alignment
- No data loss during union

**Test Data**: 2 circuits from Utility1, 2 from Utility2

**Assertions**:
- Total count = 4 circuits
- All circuit IDs present
- No duplicate circuits

#### 4. `test_null_and_empty_circuit_id_filtering`
**Purpose**: Validate filtering of invalid circuit IDs

**Data Quality Rules**:
- `circuit_id IS NOT NULL`
- `TRIM(circuit_id) != ''`

**Test Data**: Valid, NULL, empty string, and whitespace circuit IDs

**Assertions**:
- Only 2 valid circuits remain
- Invalid records filtered out

#### 5. `test_voltage_validation`
**Purpose**: Validate voltage values are positive

**Data Quality Rules**:
- `voltage_kv > 0`

**Test Data**: Valid, negative, and zero voltages

**Assertions**:
- Only positive voltages remain
- Negative and zero values filtered

#### 6. `test_capacity_validation`
**Purpose**: Validate capacity and load values

**Data Quality Rules**:
- `total_capacity_mva > 0`
- `avg_load_mva >= 0` (zero load acceptable)

**Test Data**: Valid, negative capacity, negative load, zero load

**Assertions**:
- Invalid capacity/load filtered
- Zero load is acceptable

#### 7. `test_utilization_calculation`
**Purpose**: Validate circuit utilization percentage calculation

**Business Logic**:
- `utilization_pct = (avg_load_mva / total_capacity_mva) * 100`

**Test Data**: Various utilization scenarios (34.25%, 75%, 100%, 0%)

**Assertions**:
- Correct calculation for all scenarios
- Edge case (0% utilization) handled

#### 8. `test_deduplication`
**Purpose**: Validate handling of duplicate circuit records

**Business Logic**:
- Keep most recent record based on `ingestion_timestamp`
- Each `circuit_id` appears only once

**Test Data**: Duplicate CKT001 with different timestamps

**Assertions**:
- 2 unique circuits after deduplication
- Most recent record kept for duplicates

---

### test_planned_der_silver_layer.py (10 tests)

#### 1. `test_circuit_id_validation`
**Purpose**: Validate only records with valid circuit IDs are processed

**Data Quality Rules**:
- `circuit_id IS NOT NULL`
- `TRIM(circuit_id) != ''`

**Test Data**: Valid, NULL, empty, and whitespace circuit IDs

**Assertions**: Only valid circuit IDs remain

#### 2. `test_date_parsing_and_validation`
**Purpose**: Validate planned commission date parsing

**Business Logic**:
- Parse `planned_commission_date` (YYYY-MM-DD) to date type
- Invalid dates → NULL

**Test Data**: Valid dates, invalid date format, NULL

**Assertions**:
- 3 valid dates parsed correctly
- Invalid dates result in NULL

#### 3. `test_project_status_filtering`
**Purpose**: Validate filtering by project status

**Business Logic**:
- Include only 'Planned' or 'Approved' projects
- Exclude 'Cancelled', 'Completed', 'On Hold'

**Test Data**: Various project statuses

**Assertions**:
- Only 'Planned' and 'Approved' projects remain
- Other statuses filtered

#### 4. `test_capacity_validation_and_conversion`
**Purpose**: Validate capacity values are positive and numeric

**Data Quality Rules**:
- `capacity_kw > 0`
- Convert string to double

**Test Data**: Valid, negative, zero, decimal, non-numeric values

**Assertions**:
- Only positive numeric capacities remain
- Non-numeric and invalid values filtered

#### 5. `test_technology_type_categorization`
**Purpose**: Validate technology type standardization

**Business Logic**:
- Standardize case variations (Solar, SOLAR → Solar)
- Map aliases (PV → Solar, BESS → Battery)

**Test Data**: Various technology type spellings and aliases

**Assertions**:
- All variations standardized correctly
- Solar, Wind, Battery types recognized

#### 6. `test_capacity_aggregation_by_circuit`
**Purpose**: Validate aggregation of planned capacity by circuit

**Business Logic**:
- `SUM(capacity_kw)` per circuit
- `COUNT(project_id)` per circuit

**Test Data**: 3 projects on CKT001, 1 on CKT002, 1 on CKT003

**Assertions**:
- CKT001: total = 1750 kW (500 + 250 + 1000), count = 3
- CKT002: total = 750 kW, count = 1

#### 7. `test_duplicate_project_id_handling`
**Purpose**: Validate handling of duplicate project IDs

**Business Logic**:
- Keep most recent record based on `ingestion_timestamp`

**Test Data**: Duplicate PRJ001 with different timestamps

**Assertions**: Most recent record kept

#### 8. `test_commission_date_range_validation`
**Purpose**: Validate commission dates are in valid range

**Business Logic**:
- Commission dates should be future dates
- Filter past commission dates

**Test Data**: Future dates, past dates

**Assertions**:
- Only future dates remain
- Past dates filtered

---

### test_install_der_silver_layer.py (10 tests)

#### 1. `test_installation_id_validation`
**Purpose**: Validate installation ID data quality

**Data Quality Rules**:
- `installation_id IS NOT NULL`
- `TRIM(installation_id) != ''`

**Test Data**: Valid, NULL, empty, whitespace IDs

**Assertions**: Only valid installation IDs remain

#### 2. `test_commissioning_date_parsing`
**Purpose**: Validate commission date parsing

**Business Logic**:
- Parse `commission_date` (YYYY-MM-DD) to date type

**Test Data**: Valid dates, invalid format, NULL

**Assertions**:
- 3 valid dates parsed
- Invalid dates → NULL

#### 3. `test_capacity_validation_and_conversion`
**Purpose**: Validate installation capacity values

**Data Quality Rules**:
- `capacity_kw > 0`
- Convert string to double

**Test Data**: Valid, negative, zero, decimal, non-numeric

**Assertions**: Only positive numeric capacities remain

#### 4. `test_technology_type_standardization`
**Purpose**: Validate technology type standardization

**Business Logic**:
- Standardize case variations
- Map aliases (PV → Solar, BESS → Battery)

**Test Data**: Various technology type spellings

**Assertions**:
- 3 Solar (Solar, SOLAR, PV)
- 2 Wind (Wind, wind)
- 2 Battery (Battery, BESS)

#### 5. `test_capacity_aggregation_by_circuit`
**Purpose**: Validate aggregation of installed capacity by circuit

**Business Logic**:
- `SUM(capacity_kw)` per circuit
- `COUNT(installation_id)` per circuit

**Test Data**: 3 installations on CKT001, 1 on CKT002, 1 on CKT003

**Assertions**:
- CKT001: total = 850 kW (250 + 100 + 500), count = 3

#### 6. `test_capacity_aggregation_by_technology_type`
**Purpose**: Validate aggregation by technology type

**Business Logic**:
- `SUM(capacity_kw)` per technology
- `AVG(capacity_kw)` per technology
- `COUNT(installation_id)` per technology

**Test Data**: 2 Solar, 2 Wind, 1 Battery

**Assertions**:
- Solar: total = 550 kW, avg = 275 kW, count = 2
- Wind: total = 1250 kW, avg = 625 kW, count = 2

#### 7. `test_location_validation`
**Purpose**: Validate location data quality

**Data Quality Rules**:
- `location IS NOT NULL`
- `TRIM(location) != ''`

**Test Data**: Valid, NULL, empty, whitespace locations

**Assertions**: Only valid locations remain

#### 8. `test_commissioning_date_chronology`
**Purpose**: Validate commission dates are within reasonable range

**Business Logic**:
- Commission dates within 2 years of reference date
- Flag suspiciously far future dates

**Test Data**: Past dates, near future, far future (2030)

**Assertions**:
- 3 installations within reasonable range
- Far future date filtered

#### 9. `test_duplicate_installation_id_handling`
**Purpose**: Validate handling of duplicate installation IDs

**Business Logic**:
- Keep most recent record based on `ingestion_timestamp`

**Test Data**: Duplicate INST001 with updated capacity

**Assertions**: Most recent record kept

---

## Running the Tests

### Option 1: Run All Silver Layer Tests

```bash
# Run all silver layer tests
pytest tests/test_*_silver_layer.py -v

# Run with coverage
pytest tests/test_*_silver_layer.py --cov=src/pipelines/IEDR_Pipeline/transformations/silver --cov-report=html
```

### Option 2: Run Specific Test File

```bash
# Circuits tests only
pytest tests/test_circuits_silver_layer.py -v

# Planned DER tests only
pytest tests/test_planned_der_silver_layer.py -v

# Installed DER tests only
pytest tests/test_install_der_silver_layer.py -v
```

### Option 3: Run Specific Test

```bash
# Run a single test
pytest tests/test_circuits_silver_layer.py::TestCircuitsSilverLayer::test_utility1_segment_aggregation_to_circuit -v
```

### Option 4: Using Databricks Asset Bundles

```bash
# From project root
cd /Users/prakash1857@gmail.com/feature_iedr_data_pipelines/iedr_project
databricks bundle test
```

## Prerequisites

```bash
# Install test dependencies
pip install pytest databricks-connect databricks-sdk pyspark

# Configure Databricks authentication
databricks auth login
```

## Test Patterns and Best Practices

### AAA Pattern (Arrange, Act, Assert)

All tests follow the Arrange-Act-Assert pattern:

```python
def test_example(self, spark):
    # Arrange: Set up test data and schema
    data = [...]
    schema = StructType([...])
    df = spark.createDataFrame(data, schema)
    
    # Act: Apply transformation
    result = df.filter(...)
    
    # Assert: Verify expectations
    assert result.count() == expected_count
```

### Using Fixtures

Tests use the `spark` fixture from `conftest.py`:

```python
@pytest.fixture()
def spark() -> SparkSession:
    return DatabricksSession.builder.getOrCreate()
```

### Assertion Best Practices

- Use descriptive assertion messages
- Use `pytest.approx()` for floating-point comparisons
- Test both positive and negative cases
- Include edge cases (NULL, empty, zero)

## Data Quality Rules Tested

### Common Rules Across All Tests

1. **ID Validation**:
   - IDs must not be NULL
   - IDs must not be empty strings
   - IDs must not be whitespace only

2. **Numeric Validation**:
   - Capacity values must be > 0
   - Voltage values must be > 0
   - Type conversion from string to numeric

3. **Date Validation**:
   - Dates must parse correctly (YYYY-MM-DD format)
   - Invalid dates result in NULL
   - Dates should be within reasonable ranges

4. **Deduplication**:
   - Each ID appears only once in final output
   - Keep most recent record based on timestamp

5. **Technology Type Standardization**:
   - Case-insensitive matching
   - Alias mapping (PV → Solar, BESS → Battery)

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Silver Layer Tests

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
          pip install databricks-connect databricks-sdk pytest pytest-cov
      - name: Run silver layer tests
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks auth login
          pytest tests/test_*_silver_layer.py -v --cov --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## Troubleshooting

### Issue: Spark session not available
**Solution**: Ensure Databricks Connect is configured: `databricks auth login`

### Issue: Module not found errors
**Solution**: Run from project root directory where `src/` is accessible

### Issue: Tests fail with "RESOURCE_EXHAUSTED"
**Solution**: Wait a moment and retry, or reduce parallel test execution

### Issue: Date parsing failures
**Solution**: Ensure date format matches YYYY-MM-DD in test data

## Next Steps

1. **Add Integration Tests**: Test cross-layer joins (circuits ↔ DER)
2. **Add Performance Tests**: Benchmark transformation performance
3. **Add Fixture Files**: Create JSON fixtures for reusable test data
4. **Increase Coverage**: Add tests for error handling and edge cases
5. **Add Gold Layer Tests**: Test final analytics transformations

## Resources

- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
- [pytest Documentation](https://docs.pytest.org/)
- [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [Databricks Asset Bundles Testing](https://docs.databricks.com/dev-tools/bundles/testing.html)

---

**Last Updated**: 2026-05-25  
**Test Suite Version**: 1.0  
**Total Tests**: 29  
**Code Coverage**: Silver layer transformations
