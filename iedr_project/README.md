# IEDR Pipeline Project

Integrated Electricity Distribution Resource (IEDR) data pipeline for processing and analyzing circuit, planned DER, and installed DER data from multiple utility sources.

## 📋 Project Overview

The IEDR Pipeline is a Databricks Lakeflow pipeline that ingests, transforms, and analyzes electricity distribution network data. The pipeline implements a medallion architecture (Bronze, Silver, Gold) to process data from multiple utility sources and distributed energy resources (DER).

### Key Features

* **Multi-Source Data Integration**: Combines data from Utility1 (segment-level) and Utility2 (circuit-level)
* **DER Tracking**: Monitors both planned and installed distributed energy resources
* **Data Quality**: Comprehensive validation and cleansing rules
* **SCD Type 1**: Slowly Changing Dimension implementation for circuit tracking
* **Serverless**: Photon-enabled serverless compute for optimal performance
* **Fully Tested**: 29 comprehensive tests covering all silver layer transformations

## 🏗️ Architecture

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                                 │
│              (Raw Data Ingestion - Auto Loader)                      │
├─────────────────────────────────────────────────────────────────────┤
│  • utility1_circuits_bronze   (Segment-level data)                   │
│  • utility2_circuits_bronze   (Circuit-level data)                   │
│  • planned_der_bronze         (Planned projects)                     │
│  • install_der_bronze         (Installed systems)                    │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                                 │
│            (Cleansed, Validated, Unified Data)                       │
├─────────────────────────────────────────────────────────────────────┤
│  • circuits                   (Unified circuit data)                 │
│  • circuits_scd              (SCD Type 1 tracking)                   │
│  • planned_der               (Validated planned projects)            │
│  • install_der               (Validated installations)               │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                                  │
│              (Analytics-Ready, Aggregated Data)                      │
├─────────────────────────────────────────────────────────────────────┤
│  • circuit                   (Circuit analytics)                     │
│  • circuit_der               (Circuit-DER relationships)             │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Bronze Layer**: Auto Loader continuously ingests raw data from source systems
2. **Silver Layer**: Data is validated, cleansed, and unified across sources
3. **Gold Layer**: Business-ready analytics tables with aggregations and KPIs

## 📊 Data Sources

### Utility1 - Segment-Level Circuit Data
- **Granularity**: Circuit segments
- **Key Fields**: circuit_id, segment_id, voltage_kv, length_km, capacity_mva, load_mva
- **Transformation**: Aggregated to circuit level (SUM length, AVG load)

### Utility2 - Circuit-Level Data
- **Granularity**: Circuits
- **Key Fields**: circuit_id, voltage_kv, total_length_km, total_capacity_mva, avg_load_mva
- **Transformation**: Normalized and unioned with Utility1 data

### Planned DER Projects
- **Purpose**: Track future distributed energy resource installations
- **Key Fields**: project_id, circuit_id, technology_type, capacity_kw, planned_commission_date, project_status
- **Filters**: Only 'Planned' or 'Approved' projects

### Installed DER Systems
- **Purpose**: Track operational distributed energy resources
- **Key Fields**: installation_id, circuit_id, technology_type, capacity_kw, commission_date, location
- **Aggregations**: Total capacity by circuit and technology type

## 🗂️ Project Structure

```
iedr_project/
├── src/
│   └── pipelines/
│       └── IEDR_Pipeline/
│           ├── schemas/
│           │   └── bronze_schemas.py              # Schema definitions for bronze layer
│           └── transformations/
│               ├── bronze/
│               │   └── bronze_layer_auto_loader.py  # Auto Loader ingestion
│               ├── silver/
│               │   ├── circuits_silver_layer.py     # Circuit data processing
│               │   ├── circuits_silver_scd.py       # SCD Type 1 implementation
│               │   ├── planned_der_silver_layer.py  # Planned DER processing
│               │   └── install_der_silver_layer.py  # Installed DER processing
│               └── gold/
│                   ├── circuit_gold_layer.py        # Circuit analytics
│                   └── circuit_der_gold_layer.py    # Circuit-DER analytics
├── tests/
│   ├── conftest.py                              # Pytest configuration and fixtures
│   ├── test_circuits_silver_layer.py            # Circuits transformation tests (9 tests)
│   ├── test_planned_der_silver_layer.py         # Planned DER tests (10 tests)
│   ├── test_install_der_silver_layer.py         # Installed DER tests (10 tests)
│   └── README.md                                # Comprehensive test documentation
├── resources/
│   ├── jobs/
│   │   └── job.yml                              # Job configuration
│   ├── pipelines/
│   │   └── pipeline.yml                         # Pipeline configuration
│   └── variables/
│       └── variables.yml                        # Environment variables
├── fixtures/
│   └── .gitkeep                                 # Test data fixtures directory
├── databricks.yml                               # DABs bundle configuration
├── pyproject.toml                               # Python project dependencies
└── README.md                                    # This file
```

## 🔧 Key Transformations

### Bronze Layer - Auto Loader Ingestion

**Function**: `bronze_layer_auto_loader.py`

Continuously ingests raw data files from cloud storage using Auto Loader:
- Automatic schema inference and evolution
- Exactly-once processing guarantees
- Checkpointing for incremental loads
- Adds ingestion metadata (timestamp, source file)

### Silver Layer - Circuits Processing

**Function**: `circuits_silver_layer.py`

**Utility1 Processing**:
```python
# Aggregate segment-level to circuit-level
GROUP BY circuit_id
  SUM(length_km) as total_length_km
  SUM(capacity_mva) as total_capacity_mva
  AVG(load_mva) as avg_load_mva
  AVG(voltage_kv) as voltage_kv
```

**Utility2 Processing**:
- Normalize column names
- Cast string types to numeric (double)
- Standardize to match Utility1 schema

**Union & Quality**:
- Union Utility1 and Utility2 data
- Filter NULL/empty circuit_ids
- Validate positive voltages and capacities
- Calculate utilization percentage: `(avg_load / total_capacity) * 100`
- Deduplicate keeping most recent record

### Silver Layer - Planned DER Processing

**Function**: `planned_der_silver_layer.py`

**Validations**:
- Circuit ID: NOT NULL, not empty
- Capacity: > 0 kW, numeric conversion
- Commission Date: Valid date format (YYYY-MM-DD), future dates only
- Project Status: Include only 'Planned' or 'Approved'
- Technology Type: Standardize (Solar/SOLAR/PV → Solar, BESS → Battery)

**Aggregations**:
```python
# Total planned capacity per circuit
GROUP BY circuit_id
  SUM(capacity_kw) as total_planned_capacity_kw
  COUNT(project_id) as project_count
```

### Silver Layer - Installed DER Processing

**Function**: `install_der_silver_layer.py`

**Validations**:
- Installation ID: NOT NULL, not empty, unique
- Capacity: > 0 kW, numeric conversion
- Commission Date: Valid date format, within reasonable range
- Location: NOT NULL, not empty
- Technology Type: Standardize aliases

**Aggregations**:
```python
# By Circuit
GROUP BY circuit_id
  SUM(capacity_kw) as total_installed_capacity_kw
  COUNT(installation_id) as installation_count

# By Technology
GROUP BY technology_type
  SUM(capacity_kw) as total_capacity_kw
  AVG(capacity_kw) as avg_capacity_kw
  COUNT(installation_id) as installation_count
```

### Silver Layer - SCD Type 1

**Function**: `circuits_silver_scd.py`

Implements Slowly Changing Dimension Type 1 for circuit tracking:
- Overwrite existing records with latest values
- Maintain single current state per circuit
- Track update timestamps
- Preserve circuit history in audit tables (if configured)

### Gold Layer - Circuit Analytics

**Function**: `circuit_gold_layer.py`

Creates analytics-ready circuit data:
- Circuit utilization metrics
- Capacity vs. load analysis
- Voltage level categorization
- Geographic aggregations
- Time-series trends

### Gold Layer - Circuit-DER Analytics

**Function**: `circuit_der_gold_layer.py`

Combines circuit and DER data for comprehensive analysis:
- DER penetration by circuit (installed capacity / circuit capacity)
- Planned vs. installed capacity comparison
- Technology mix analysis
- Hosting capacity assessment
- Geographic DER distribution

## 🧪 Testing

### Test Suite (29 Tests, ~1,200 Lines)

Comprehensive pytest-based test suite covering all silver layer transformations:

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `test_circuits_silver_layer.py` | 9 | Aggregation, union, validation, utilization |
| `test_planned_der_silver_layer.py` | 10 | Validation, dates, status, capacity, technology |
| `test_install_der_silver_layer.py` | 10 | Validation, dates, capacity, location, aggregation |

### Running Tests

```bash
# All tests
pytest tests/ -v

# Specific layer
pytest tests/test_circuits_silver_layer.py -v

# With coverage
pytest tests/ --cov=src/pipelines/IEDR_Pipeline --cov-report=html

# Using Databricks Asset Bundles
databricks bundle test
```

### Test Coverage

- ✅ Data quality validations (NULL, empty, positive values)
- ✅ Type conversions (string → numeric)
- ✅ Date parsing and validation
- ✅ Business logic (aggregations, calculations)
- ✅ Deduplication strategies
- ✅ Technology type standardization
- ✅ Edge cases and error handling



### Data Quality Expectations

The pipeline includes data quality checks:

**Circuits**:
- `circuit_id IS NOT NULL`
- `voltage_kv > 0`
- `total_capacity_mva > 0`
- `utilization_pct BETWEEN 0 AND 100`

**Planned DER**:
- `project_id IS NOT NULL`
- `capacity_kw > 0`
- `project_status IN ('Planned', 'Approved')`
- `planned_commission_date IS NOT NULL`

**Installed DER**:
- `installation_id IS NOT NULL`
- `capacity_kw > 0`
- `commission_date IS NOT NULL`
- `location IS NOT NULL`

## 📊 Data Catalog

### Tables Created

#### Bronze Layer
- `iedr.bronze.utility1_circuits`
- `iedr.bronze.utility2_circuits`
- `iedr.bronze.planned_der`
- `iedr.bronze.install_der`

#### Silver Layer
- `iedr.silver.circuits` - Unified circuit data from all utilities
- `iedr.silver.circuits_scd` - SCD Type 1 tracked circuits
- `iedr.silver.planned_der` - Validated planned DER projects
- `iedr.silver.install_der` - Validated installed DER systems

#### Gold Layer
- `iedr.gold.circuit` - Circuit analytics and KPIs
- `iedr.gold.circuit_der` - Circuit-DER relationship analytics

### Schema Details

**circuits** (Silver):
```
circuit_id             STRING    (Primary Key)
voltage_kv            DOUBLE    
total_length_km       DOUBLE    
total_capacity_mva    DOUBLE    
avg_load_mva         DOUBLE    
utilization_pct      DOUBLE    (Calculated: avg_load/capacity * 100)
source_system        STRING    (utility1 or utility2)
ingestion_timestamp  TIMESTAMP
update_timestamp     TIMESTAMP
```

**planned_der** (Silver):
```
project_id                  STRING    (Primary Key)
circuit_id                 STRING    (Foreign Key → circuits.circuit_id)
technology_type            STRING    (Solar, Wind, Battery)
capacity_kw               DOUBLE    
planned_commission_date    DATE      
project_status            STRING    (Planned, Approved)
ingestion_timestamp       TIMESTAMP
```

**install_der** (Silver):
```
installation_id       STRING    (Primary Key)
circuit_id           STRING    (Foreign Key → circuits.circuit_id)
technology_type      STRING    (Solar, Wind, Battery)
capacity_kw         DOUBLE    
commission_date      DATE      
location            STRING    
ingestion_timestamp TIMESTAMP
```

## 📚 Additional Resources

### Documentation
- [Databricks Lakeflow Pipelines](https://docs.databricks.com/delta-live-tables/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
