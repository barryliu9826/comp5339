#!/usr/bin/env python3
"""
JSON to CSV Converter for Energy Data
Convert energy JSON files to CSV format

Author: Principal AI/ML Systems Engineer Agent
"""

import json
import pandas as pd
import argparse
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import logging

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONToCSVConverter:
    """Core class for converting JSON data to CSV"""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "data/csv_output") -> None:
        """
        Initialize converter.
        
        Args:
            data_dir: Directory of JSON source files
            output_dir: Directory for CSV outputs
        """
        project_root = Path(__file__).resolve().parents[2]
        self.data_dir = (project_root / data_dir).resolve()
        self.output_dir = (project_root / output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Data file paths
        self.facilities_file = self.data_dir / "facilities_data.json"
        self.metrics_file = self.data_dir / "facility_metrics.json"
        self.market_file = self.data_dir / "market_data.json"
        
    def load_json_data(self, file_path: Path) -> Dict[str, Any]:
        """
        Load a JSON data file.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Parsed JSON data
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded file: {file_path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            raise
    
    def convert_facilities_data(self) -> None:
        """
        Convert facilities data to CSV format.
        
        Transform nested JSON into two relational CSVs:
        - facilities.csv: facilities master table (flattened)
        - facility_units.csv: facility units table (all unit fields)
        
        Raises:
            FileNotFoundError: JSON file not found
            KeyError: required field missing
            json.JSONDecodeError: JSON decode failed
        """
        logger.info("Starting facilities conversion...")
        
        # Load data
        data = self.load_json_data(self.facilities_file)
        
        # Validate top-level structure
        if not isinstance(data, dict):
            error_msg = f"Invalid JSON structure: expected dict, got {type(data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "Missing 'data' field in JSON file"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        facilities_data = data.get('data', [])
        
        if not isinstance(facilities_data, list):
            error_msg = f"'data' must be a list, got {type(facilities_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Prepare records
        facilities_records: List[Dict[str, Any]] = []
        units_records: List[Dict[str, Any]] = []
        
        for facility in facilities_data:
            # Validate required facility fields
            if 'code' not in facility:
                error_msg = f"Facility record missing required field 'code': {facility}"
                logger.error(error_msg)
                raise KeyError(error_msg)
            
            # Extract base info (flatten 'location')
            location = facility.get('location', {})
            if not isinstance(location, dict):
                location = {}
            
            facility_record = {
                'code': facility.get('code'),
                'name': facility.get('name'),
                'network_id': facility.get('network_id'),
                'network_region': facility.get('network_region'),
                'description': facility.get('description', ''),
                'npi_id': facility.get('npi_id'),
                'lat': location.get('lat') if isinstance(location, dict) else None,
                'lng': location.get('lng') if isinstance(location, dict) else None,
                'created_at': facility.get('created_at'),
                'updated_at': facility.get('updated_at')
            }
            facilities_records.append(facility_record)
            
            # Extract unit information (all fields)
            units = facility.get('units', [])
            if not isinstance(units, list):
                units = []
            
            facility_code = facility.get('code')
            
            for unit in units:
                if not isinstance(unit, dict):
                    logger.warning(f"Skip invalid unit record (facility: {facility_code}): {type(unit).__name__}")
                    continue
                
                # Extract all unit fields (including date-related fields)
                unit_record = {
                    'facility_code': facility_code,
                    'unit_code': unit.get('code'),
                    'fueltech_id': unit.get('fueltech_id'),
                    'status_id': unit.get('status_id'),
                    'capacity_registered': unit.get('capacity_registered'),
                    'capacity_maximum': unit.get('capacity_maximum'),
                    'capacity_storage': unit.get('capacity_storage'),
                    'emissions_factor_co2': unit.get('emissions_factor_co2'),
                    'data_first_seen': unit.get('data_first_seen'),
                    'data_last_seen': unit.get('data_last_seen'),
                    'dispatch_type': unit.get('dispatch_type'),
                    # Commencement date fields
                    'commencement_date': unit.get('commencement_date'),
                    'commencement_date_specificity': unit.get('commencement_date_specificity'),
                    'commencement_date_display': unit.get('commencement_date_display'),
                    # Closure date fields
                    'closure_date': unit.get('closure_date'),
                    'closure_date_specificity': unit.get('closure_date_specificity'),
                    'closure_date_display': unit.get('closure_date_display'),
                    # Expected operation date fields
                    'expected_operation_date': unit.get('expected_operation_date'),
                    'expected_operation_date_specificity': unit.get('expected_operation_date_specificity'),
                    'expected_operation_date_display': unit.get('expected_operation_date_display'),
                    # Expected closure date fields
                    'expected_closure_date': unit.get('expected_closure_date'),
                    'expected_closure_date_specificity': unit.get('expected_closure_date_specificity'),
                    'expected_closure_date_display': unit.get('expected_closure_date_display'),
                    # Construction start date fields
                    'construction_start_date': unit.get('construction_start_date'),
                    'construction_start_date_specificity': unit.get('construction_start_date_specificity'),
                    'construction_start_date_display': unit.get('construction_start_date_display'),
                    # Project approval date fields
                    'project_approval_date': unit.get('project_approval_date'),
                    'project_approval_date_specificity': unit.get('project_approval_date_specificity'),
                    'project_approval_date_display': unit.get('project_approval_date_display'),
                    # Project lodgement date
                    'project_lodgement_date': unit.get('project_lodgement_date'),
                    # Metadata timestamps
                    'created_at': unit.get('created_at'),
                    'updated_at': unit.get('updated_at')
                }
                units_records.append(unit_record)
        
        # Create DataFrames and save to CSV
        facilities_df = pd.DataFrame(facilities_records)
        units_df = pd.DataFrame(units_records)
        
        # Save CSV files (use NA for missing values)
        facilities_csv_path = self.output_dir / "facilities.csv"
        units_csv_path = self.output_dir / "facility_units.csv"
        
        facilities_df.to_csv(facilities_csv_path, index=False, encoding='utf-8', na_rep='NA')
        units_df.to_csv(units_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        logger.info(f"Facilities conversion completed: {len(facilities_records)} facilities, {len(units_records)} units")
        logger.info(f"Output files: {facilities_csv_path}, {units_csv_path}")
        logger.info(f"Facility columns: {len(facilities_df.columns)}, Unit columns: {len(units_df.columns)}")
    
    def convert_facility_metrics_data(self) -> None:
        """
        Convert facility metrics to wide-format CSV.
        
        Target structure: each row includes facility_code, timestamp, power, emissions, longitude, latitude
        
        Steps:
        1. Load JSON and build a long-form DataFrame
        2. Pivot to wide format (power and emissions as columns)
        3. Join facilities data to add longitude/latitude
        4. Validate data integrity
        5. Save as wide-format CSV
        
        Raises:
            ValueError: invalid data format
            KeyError: missing required fields
            FileNotFoundError: facilities CSV not found
        """
        logger.info("Starting facility metrics conversion (wide format)...")
        
        # Load JSON data
        data = self.load_json_data(self.metrics_file)
        
        # Validate top-level structure
        if not isinstance(data, dict):
            error_msg = f"Invalid JSON structure: expected dict, got {type(data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "Missing 'data' field in JSON file"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        metrics_data = data.get('data', {})
        
        if not isinstance(metrics_data, dict):
            error_msg = f"'data' field must be dict type, actual: {type(metrics_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Prepare data records (long format)
        data_records: List[Dict[str, Any]] = []
        
        # Iterate each metric type (e.g., power, emissions)
        for metric_name, metric_data in metrics_data.items():
            if not isinstance(metric_data, dict):
                logger.warning(f"Skip invalid metric data: {metric_name} (type: {type(metric_data).__name__})")
                continue
            
            # Extract facilities data
            facilities = metric_data.get('facilities', {})
            
            if not isinstance(facilities, dict):
                logger.warning(f"Skip invalid facilities data: {metric_name} (type: {type(facilities).__name__})")
                continue
            
            # Iterate time series per facility
            for facility_code, time_series in facilities.items():
                if not isinstance(time_series, dict):
                    logger.warning(f"Skip invalid time series: {metric_name}/{facility_code}")
                    continue
                
                # Iterate timestamps and values
                for timestamp, value in time_series.items():
                    data_record = {
                        'facility_code': facility_code,
                        'timestamp': timestamp,
                        'metric': metric_name,
                        'value': value
                    }
                    data_records.append(data_record)
        
        if not data_records:
            error_msg = "No valid metric data found"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Build long-form DataFrame
        long_df = pd.DataFrame(data_records)
        
        logger.info(f"Long-form rows: {len(long_df)}")
        logger.info(f"Unique facilities: {long_df['facility_code'].nunique()}")
        logger.info(f"Unique timestamps: {long_df['timestamp'].nunique()}")
        
        # Step 1: Pivot (Long to Wide)
        # Use 'metric' as columns and 'value' as values
        wide_df = long_df.pivot_table(
            index=['facility_code', 'timestamp'],
            columns='metric',
            values='value',
            aggfunc='first',  # if duplicates exist, take first
            fill_value=None  # missing values as None (later becomes NA)
        )
        
        # Reset index to columns
        wide_df = wide_df.reset_index()
        
        # Remove column index name if present
        wide_df.columns.name = None
        
        logger.info(f"Wide-form rows: {len(wide_df)}")
        logger.info(f"Wide-form columns: {list(wide_df.columns)}")
        
        # Step 2: join facilities to add geolocation
        facilities_csv_path = self.output_dir / "facilities.csv"
        
        if not facilities_csv_path.exists():
            error_msg = f"Facilities CSV not found: {facilities_csv_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Read facilities CSV
        facilities_df = pd.read_csv(facilities_csv_path)
        
        # Validate required columns
        required_cols = ['code', 'lat', 'lng']
        missing_cols = [col for col in required_cols if col not in facilities_df.columns]
        if missing_cols:
            error_msg = f"Facilities CSV missing required columns: {missing_cols}"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        # Left join: keep all metrics rows, add geolocation
        wide_df = wide_df.merge(
            facilities_df[['code', 'lat', 'lng']],
            left_on='facility_code',
            right_on='code',
            how='left'
        )
        
        # Step 3: rename and order columns
        # Rename lat → latitude, lng → longitude
        wide_df = wide_df.rename(columns={'lat': 'latitude', 'lng': 'longitude'})
        
        # Drop code column (joined already, no longer needed)
        if 'code' in wide_df.columns:
            wide_df = wide_df.drop(columns=['code'])
        
        # Ensure column order: facility_code, timestamp, power, emissions, longitude, latitude
        # If some metrics are missing, still include the columns
        column_order = ['facility_code', 'timestamp']
        
        # Add metric columns (in order: power, emissions)
        metric_order = ['power', 'emissions']
        metric_cols = [col for col in metric_order if col in wide_df.columns]
        column_order.extend(metric_cols)
        
        # Add geolocation columns
        column_order.extend(['longitude', 'latitude'])
        
        # Ensure all columns exist; create as NA if missing
        for col in column_order:
            if col not in wide_df.columns:
                wide_df[col] = None
        
        # Reorder columns
        wide_df = wide_df[column_order]
        
        # Sort by facility_code and timestamp
        wide_df = wide_df.sort_values(['facility_code', 'timestamp'])
        
        # Step 4: validation
        expected_rows = wide_df['facility_code'].nunique() * wide_df['timestamp'].nunique()
        actual_rows = len(wide_df)
        
        if actual_rows != expected_rows:
            logger.warning(f"Row count mismatch: expected {expected_rows}, actual {actual_rows}")
        
        # Missing values
        missing_power = wide_df['power'].isna().sum()
        missing_emissions = wide_df['emissions'].isna().sum()
        missing_longitude = wide_df['longitude'].isna().sum()
        missing_latitude = wide_df['latitude'].isna().sum()
        
        logger.info(f"Integrity check:")
        logger.info(f"  - total rows: {actual_rows}")
        logger.info(f"  - missing power: {missing_power} ({missing_power/actual_rows*100:.2f}%)")
        logger.info(f"  - missing emissions: {missing_emissions} ({missing_emissions/actual_rows*100:.2f}%)")
        logger.info(f"  - missing longitude: {missing_longitude} ({missing_longitude/actual_rows*100:.2f}%)")
        logger.info(f"  - missing latitude: {missing_latitude} ({missing_latitude/actual_rows*100:.2f}%)")
        
        # Unmatched facility codes (no geolocation)
        unmatched_facilities = wide_df[wide_df['longitude'].isna()]['facility_code'].unique()
        if len(unmatched_facilities) > 0:
            logger.warning(f"Facilities without geolocation: {len(unmatched_facilities)}")
            logger.warning(f"First 10 unmatched facilities: {list(unmatched_facilities[:10])}")
        
        # Step 5: save as wide CSV
        wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
        
        wide_df.to_csv(wide_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        logger.info(f"Facility metrics conversion completed (wide): {len(wide_df)} rows, {len(wide_df.columns)} columns")
        logger.info(f"Output file: {wide_csv_path}")
        logger.info(f"Column order: {', '.join(wide_df.columns)}")
    
    def convert_market_data(self) -> None:
        """Convert market data to CSV format"""
        logger.info("Starting market data conversion...")
        
        # Load data
        data = self.load_json_data(self.market_file)
        market_data = data.get('data', [])
        
        # Prepare metadata and data records
        metadata_records = []
        data_records = []
        
        for metric_group in market_data:
            # Extract metadata
            metadata_record = {
                'network_code': metric_group.get('network_code'),
                'metric': metric_group.get('metric'),
                'unit': metric_group.get('unit'),
                'interval': metric_group.get('interval'),
                'date_start': metric_group.get('date_start'),
                'date_end': metric_group.get('date_end'),
                'network_timezone_offset': metric_group.get('network_timezone_offset')
            }
            metadata_records.append(metadata_record)
            
            # Extract time series data
            results = metric_group.get('results', [])
            for result in results:
                data_points = result.get('data', [])
                
                for data_point in data_points:
                    if len(data_point) >= 2:
                        data_record = {
                            'network_code': metric_group.get('network_code'),
                            'metric': metric_group.get('metric'),
                            'timestamp': data_point[0],
                            'value': data_point[1]
                        }
                        data_records.append(data_record)
        
        # Create DataFrames and save to CSV
        metadata_df = pd.DataFrame(metadata_records)
        data_df = pd.DataFrame(data_records)
        
        # Save CSV files
        metadata_csv_path = self.output_dir / "market_metrics_metadata.csv"
        data_csv_path = self.output_dir / "market_metrics_data.csv"
        
        metadata_df.to_csv(metadata_csv_path, index=False, encoding='utf-8')
        data_df.to_csv(data_csv_path, index=False, encoding='utf-8')
        
        logger.info(f"Market data conversion completed: {len(metadata_records)} metric groups, {len(data_records)} data points")
        logger.info(f"Output files: {metadata_csv_path}, {data_csv_path}")
    
    def validate_data_integrity(self) -> None:
        """Validate data integrity"""
        logger.info("Starting data integrity validation...")
        
        # Check expected output files exist
        expected_files = [
            "facilities.csv",
            "facility_units.csv", 
            "facility_metrics_wide.csv",
            "market_metrics_metadata.csv",
            "market_metrics_data.csv"
        ]
        
        for filename in expected_files:
            file_path = self.output_dir / filename
            if file_path.exists():
                df = pd.read_csv(file_path)
                logger.info(f"{filename}: {len(df)} rows")
            else:
                logger.warning(f"{filename}: file not found")
        
        # Validate foreign-key-like relationships
        try:
            facilities_df = pd.read_csv(self.output_dir / "facilities.csv")
            units_df = pd.read_csv(self.output_dir / "facility_units.csv")
            
            # Check facility code reference integrity
            facility_codes = set(facilities_df['code'])
            unit_facility_codes = set(units_df['facility_code'])
            
            missing_facilities = unit_facility_codes - facility_codes
            if missing_facilities:
                logger.warning(f"Unmatched unit references: {missing_facilities}")
            else:
                logger.info("Facility-unit relationship integrity check passed")
            
            # Validate facility_code in facility_metrics_wide is in facilities
            wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
            if wide_csv_path.exists():
                wide_df = pd.read_csv(wide_csv_path)
                wide_facility_codes = set(wide_df['facility_code'].unique())
                missing_in_wide = wide_facility_codes - facility_codes
                if missing_in_wide:
                    logger.warning(f"Unmatched facility_code in facility_metrics_wide: {len(missing_in_wide)}")
                else:
                    logger.info("facility_metrics_wide facility_code integrity check passed")
                
        except Exception as e:
            logger.error(f"Data integrity validation failed: {e}")
    
    def generate_summary_report(self) -> None:
        """Generate conversion summary report"""
        logger.info("Generating conversion summary report...")
        
        report_path = self.output_dir / "conversion_report.txt"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("JSON to CSV Conversion Summary\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # File stats
            csv_files = list(self.output_dir.glob("*.csv"))
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    f.write(f"{csv_file.name}: {len(df)} rows, {len(df.columns)} columns\n")
                except Exception as e:
                    f.write(f"{csv_file.name}: read failed - {e}\n")
        
        logger.info(f"Summary report generated: {report_path}")
    
    def run_conversion(self) -> None:
        """Run the full conversion workflow"""
        logger.info("Starting JSON-to-CSV conversion workflow...")
        
        try:
            # Convert datasets
            self.convert_facilities_data()
            self.convert_facility_metrics_data()
            self.convert_market_data()
            
            # Validate integrity
            self.validate_data_integrity()
            
            # Generate summary
            self.generate_summary_report()
            
            logger.info("Conversion workflow completed!")
            
        except Exception as e:
            logger.error(f"Conversion workflow failed: {e}")
            raise


def main_facilities_only() -> None:
    """
    Convert only facilities data to CSV.
    
    Converts facilities_data.json to CSV outputs:
        - csv_output/facilities.csv: facilities master table
        - csv_output/facility_units.csv: facility units table
    """
    print("Facilities JSON to CSV Converter")
    print("=" * 50)
    
    try:
        # Create converter (facilities only)
        converter = JSONToCSVConverter(data_dir="data", output_dir="data/csv_output")
        
        # Run facilities conversion only
        converter.convert_facilities_data()
        
        # Validate and show stats
        try:
            # Read source JSON to get input stats
            facilities_json_data = converter.load_json_data(converter.facilities_file)
            facilities_count = len(facilities_json_data.get('data', []))
            
            # Read generated CSVs for output stats
            output_facilities_df = pd.read_csv(converter.output_dir / "facilities.csv")
            output_units_df = pd.read_csv(converter.output_dir / "facility_units.csv")
            
            print(f"\nResults:")
            print(f"  - Input facilities: {facilities_count}")
            print(f"  - Output facility rows: {len(output_facilities_df)}")
            print(f"  - Output unit rows: {len(output_units_df)}")
            print(f"  - Facility columns: {len(output_facilities_df.columns)}")
            print(f"  - Unit columns: {len(output_units_df.columns)}")
            print(f"\nOutputs:")
            print(f"  - {converter.output_dir / 'facilities.csv'}")
            print(f"  - {converter.output_dir / 'facility_units.csv'}")
            
        except Exception as e:
            logger.warning(f"Validation skipped: {e}")
        
        print("\nConversion completed!")
        print(f"See 'conversion.log' for detailed logs")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"\nError: file not found")
        raise
    except KeyError as e:
        logger.error(f"Data format error: {e}")
        print(f"\nError: data format error - {e}")
        raise
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        print(f"\nError: conversion failed - {e}")
        raise


def main() -> None:
    """
    Entrypoint: support CLI flags to choose conversion mode.
    
    Usage:
        python json_to_csv_converter.py              # convert all (facility_metrics in wide format)
        python json_to_csv_converter.py --facilities # convert facilities only
    """
    parser = argparse.ArgumentParser(
        description="JSON to CSV Converter for Energy Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python json_to_csv_converter.py                    # convert all (facility_metrics in wide format)
  python json_to_csv_converter.py --facilities       # convert facilities only
        """
    )
    
    parser.add_argument(
        '--facilities',
        action='store_true',
        help='Convert only facilities_data.json to CSV'
    )
    
    args = parser.parse_args()
    
    # Choose mode by args
    if args.facilities:
        main_facilities_only()
    else:
        print("JSON to CSV Converter for Energy Data")
        print("=" * 50)
        
        # Create converter
        converter = JSONToCSVConverter()
        
        # Ensure facilities.csv exists (needed by wide format conversion)
        facilities_csv_path = converter.output_dir / "facilities.csv"
        if not facilities_csv_path.exists():
            logger.info("Facilities CSV missing, converting facilities first...")
            converter.convert_facilities_data()
        
        # Run full conversion (facility_metrics uses wide format by default)
        converter.run_conversion()
        
        print("\nConversion completed! Outputs at 'data/csv_output'")
        print("See 'conversion.log' for detailed logs")


if __name__ == "__main__":
    main()
