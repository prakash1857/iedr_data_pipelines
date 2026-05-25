#!/bin/bash
# IEDR Pipeline Test Runner
# This script provides convenient commands to run the IEDR Pipeline test suite

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}    IEDR Pipeline Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to display usage
usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  all              Run all tests (default)"
    echo "  bronze           Run bronze layer tests only"
    echo "  silver           Run silver layer tests only"
    echo "  gold             Run gold layer tests only"
    echo "  integration      Run integration tests only"
    echo "  quality          Run data quality tests only"
    echo "  coverage         Run tests with coverage report"
    echo "  verbose          Run tests with verbose output"
    echo "  help             Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all           # Run all tests"
    echo "  $0 silver        # Run only silver layer tests"
    echo "  $0 coverage      # Run with coverage report"
    echo ""
}

# Check if pytest is installed
check_dependencies() {
    if ! command -v pytest &> /dev/null; then
        echo -e "${RED}Error: pytest is not installed${NC}"
        echo "Please install pytest: pip install pytest"
        exit 1
    fi
    
    if ! python -c "import databricks.connect" &> /dev/null 2>&1; then
        echo -e "${YELLOW}Warning: databricks-connect not found${NC}"
        echo "Install it with: pip install databricks-connect"
    fi
}

# Run all tests
run_all_tests() {
    echo -e "${GREEN}Running all IEDR Pipeline tests...${NC}"
    pytest tests/iedr_pipeline_test.py -v --tb=short
}

# Run bronze layer tests
run_bronze_tests() {
    echo -e "${GREEN}Running Bronze Layer tests...${NC}"
    pytest tests/iedr_pipeline_test.py::TestBronzeLayer -v --tb=short
}

# Run silver layer tests
run_silver_tests() {
    echo -e "${GREEN}Running Silver Layer tests...${NC}"
    pytest tests/iedr_pipeline_test.py::TestCircuitsSilverLayer -v --tb=short
    pytest tests/iedr_pipeline_test.py::TestPlannedDERSilverLayer -v --tb=short
    pytest tests/iedr_pipeline_test.py::TestInstallDERSilverLayer -v --tb=short
}

# Run gold layer tests
run_gold_tests() {
    echo -e "${GREEN}Running Gold Layer tests...${NC}"
    pytest tests/iedr_pipeline_test.py::TestCircuitGoldLayer -v --tb=short
}

# Run integration tests
run_integration_tests() {
    echo -e "${GREEN}Running Integration tests...${NC}"
    pytest tests/iedr_pipeline_test.py::TestPipelineIntegration -v --tb=short
}

# Run data quality tests
run_quality_tests() {
    echo -e "${GREEN}Running Data Quality tests...${NC}"
    pytest tests/iedr_pipeline_test.py::TestDataQuality -v --tb=short
}

# Run with coverage
run_with_coverage() {
    echo -e "${GREEN}Running tests with coverage report...${NC}"
    pytest tests/iedr_pipeline_test.py \
        --cov=src/pipelines/IEDR_Pipeline \
        --cov-report=html \
        --cov-report=term-missing \
        -v
    echo ""
    echo -e "${BLUE}Coverage report generated at: htmlcov/index.html${NC}"
}

# Run with verbose output
run_verbose() {
    echo -e "${GREEN}Running tests with verbose output...${NC}"
    pytest tests/iedr_pipeline_test.py -vv --tb=long
}

# Main script logic
check_dependencies

case "${1:-all}" in
    all)
        run_all_tests
        ;;
    bronze)
        run_bronze_tests
        ;;
    silver)
        run_silver_tests
        ;;
    gold)
        run_gold_tests
        ;;
    integration)
        run_integration_tests
        ;;
    quality)
        run_quality_tests
        ;;
    coverage)
        run_with_coverage
        ;;
    verbose)
        run_verbose
        ;;
    help|--help|-h)
        usage
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Unknown option '$1'${NC}"
        echo ""
        usage
        exit 1
        ;;
esac

# Print summary
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Test execution completed!${NC}"
echo -e "${BLUE}========================================${NC}"
