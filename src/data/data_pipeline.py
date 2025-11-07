#!/usr/bin/env python3
"""
Unified Data Pipeline System
Complete data pipeline system for data acquisition, transformation, and publication

Functional Modules:
1. DataAcquisitionService: Acquire data from OpenElectricity API
2. DataTransformationService: Convert JSON data to CSV format
3. DataPublicationService: Publish CSV data to MQTT

Author: Principal AI/ML Systems Engineer Agent
"""

from __future__ import annotations

import os
import json
import time
import math
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Iterator
from collections import defaultdict

import pandas as pd
import paho.mqtt.client as mqtt

# Environment variables are provided externally (via ConfigLoader); do not hard-code API keys in code
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

# SDK imports
try:
    from openelectricity import OEClient
    from openelectricity.models.facilities import FacilityResponse, Facility
    from openelectricity.models.timeseries import TimeSeriesResponse, NetworkTimeSeries, TimeSeriesResult
    from openelectricity.types import (
        DataMetric,
        MarketMetric,
        NetworkCode,
        DataInterval,
        DataPrimaryGrouping,
        UnitStatusType,
    )
except ImportError as e:
    # If SDK not installed, provide friendly error message
    raise ImportError(
        "OpenElectricity SDK is required. Please install it with: pip install openelectricity"
    ) from e

# ============================================================================
# Configuration Management Module
# ============================================================================

class ConfigManager:
    """Unified configuration manager: manages all configuration items (API keys, MQTT broker, paths, etc.)"""
    
    def __init__(self) -> None:
        """Initialize configuration manager"""
        self._ensure_logs_directory()
    
    @staticmethod
    def _ensure_logs_directory() -> None:
        """Ensure logs directory exists"""
        os.makedirs('logs', exist_ok=True)
    
    @staticmethod
    def load_api_key() -> str:
        """
        Load API key (required)
        
        Returns:
            API key string
            
        Raises:
            ValueError: API key is not set
        """
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            raise ValueError("OPENELECTRICITY_API_KEY is not set")
        return api_key
    
    @staticmethod
    def load_base_url() -> Optional[str]:
        """
        Load API base URL (optional)
        
        Returns:
            API base URL, or None if not set
        """
        return os.getenv("OE_BASE_URL")
    
    @staticmethod
    def load_time_window() -> Tuple[datetime, datetime]:
        """
        Load time window (default values)
        
        Returns:
            Tuple of (start_time, end_time)
        """
        date_start = datetime(2025, 10, 1)
        date_end = datetime(2025, 10, 8)
        return date_start, date_end
    
    @staticmethod
    def load_interval() -> str:
        """
        Load data interval (default '5m')
        
        Returns:
            Data interval string
        """
        return os.getenv("INTERVAL", "5m")
    
    @staticmethod
    def load_batch_config() -> Dict[str, Any]:
        """
        Load batch processing configuration
        
        Returns:
            Dictionary containing batch_size and batch_delay
        """
        batch_size = int(os.getenv("BATCH_SIZE", "30"))
        batch_delay = float(os.getenv("BATCH_DELAY_SECS", "0.5"))
        return {
            "batch_size": batch_size,
            "batch_delay": batch_delay
        }
    
    @staticmethod
    def load_network_filter() -> Optional[str]:
        """
        Load network filter (default 'NEM')
        
        Returns:
            Network filter string
        """
        return os.getenv("NETWORK_FILTER", "NEM")
    
    @staticmethod
    def load_status_filter() -> Optional[str]:
        """
        Load status filter (default 'operating')
        
        Returns:
            Status filter string
        """
        return os.getenv("STATUS_FILTER", "operating")
    
    @staticmethod
    def load_mqtt_config() -> Dict[str, Any]:
        """
        Load MQTT configuration
        
        Returns:
            Dictionary containing MQTT broker configuration
        """
        return {
            "broker_host": os.getenv("MQTT_BROKER_HOST", "broker.hivemq.com"),
            "broker_port": int(os.getenv("MQTT_BROKER_PORT", "1883")),
            "facility_topic": os.getenv("MQTT_FACILITY_TOPIC", "a02/facility_metrics/v1/stream"),
            "market_topic": os.getenv("MQTT_MARKET_TOPIC", "a02/market_metrics/v1/stream"),
            "qos": int(os.getenv("MQTT_QOS", "0"))
        }
    
    @staticmethod
    def load_paths() -> Dict[str, str]:
        """
        Load path configuration
        
        Returns:
            Dictionary containing data directory and output directory
        """
        return {
            "data_dir": os.getenv("DATA_DIR", "data"),
            "output_dir": os.getenv("OUTPUT_DIR", "data/csv_output"),
            "batch_dir": os.getenv("BATCH_DIR", "data/batch_data")
        }


# ============================================================================
# Logging Configuration Module
# ============================================================================

class LoggerConfig:
    """Unified logging configuration"""
    
    @staticmethod
    def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
        """
        Setup logger
        
        Args:
            name: Logger name
            log_file: Log file path (optional)
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        # Avoid duplicate handlers
        if logger.handlers:
            return logger
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger


# ============================================================================
# Shared Utility Module
# ============================================================================

class FileManager:
    """File management utility class: handles JSON read/write and directory management"""
    
    @staticmethod
    def save_json(data: Dict[str, Any] | List[Dict[str, Any]], filepath: str) -> None:
        """
        Save JSON data to file
        
        Args:
            data: Data to save (dict or list of dicts)
            filepath: File path
        """
        FileManager.ensure_directory(os.path.dirname(filepath))
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Load JSON data from file
        
        Args:
            filepath: File path
            
        Returns:
            Parsed JSON data
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def ensure_directory(dirpath: str) -> None:
        """
        Ensure directory exists
        
        Args:
            dirpath: Directory path
        """
        if dirpath:
            Path(dirpath).mkdir(parents=True, exist_ok=True)


class DataValidator:
    """Data validation utility class"""
    
    @staticmethod
    def validate_metrics(response: TimeSeriesResponse) -> None:
        """
        Validate metrics data response
        
        Args:
            response: TimeSeriesResponse object
            
        Raises:
            ValueError: Response failed or data is empty
        """
        if not response.success:
            raise ValueError(f"Failed to fetch metrics data: {response.error}")
        
        if not response.data:
            raise ValueError("Metrics data is empty")


# ============================================================================
# Data Acquisition Service Module
# ============================================================================

class DataConverter:
    """Data converter: converts Pydantic models to dictionaries"""
    
    @staticmethod
    def facility_response_to_dict(response: FacilityResponse) -> Dict[str, Any]:
        """
        Convert FacilityResponse to dictionary
        
        Args:
            response: FacilityResponse object
            
        Returns:
            Data in dictionary format
        """
        return response.model_dump()
    
    @staticmethod
    def timeseries_response_to_dict(
        response: TimeSeriesResponse,
        data_type: str = "metrics"
    ) -> Dict[str, Any]:
        """
        Convert TimeSeriesResponse to dictionary
        
        Args:
            response: TimeSeriesResponse object
            data_type: Data type identifier
            
        Returns:
            Data in dictionary format
        """
        data = response.model_dump()
        data['data_type'] = data_type
        return data
    
    @staticmethod
    def add_metadata(
        data: Dict[str, Any],
        data_type: str
    ) -> Dict[str, Any]:
        """
        Add processing metadata
        
        Args:
            data: Data dictionary
            data_type: Data type
            
        Returns:
            Data dictionary with metadata added
        """
        # Add processing timestamp
        data['processed_at'] = datetime.now().isoformat()
        
        # Add data type if not already present
        if 'data_type' not in data:
            data['data_type'] = data_type
        
        # Calculate and add total record count if not already present
        if 'total_records' not in data:
            data_list = data.get('data', [])
            # Only count if data is a list
            if isinstance(data_list, list):
                data['total_records'] = len(data_list)
        
        return data
    
    @staticmethod
    def aggregated_facility_data_to_dict(
        aggregated_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Convert aggregated facility-level data to dictionary
        
        Args:
            aggregated_data: Aggregated facility-level data
            metadata: Additional metadata
            
        Returns:
            Formatted dictionary payload
        """
        result = {
            "version": "1.0",
            "aggregated_at": datetime.now().isoformat(),
            "aggregation_method": "facility_unit_mapping",
        }
        
        if metadata:
            result.update(metadata)
        
        result["data"] = aggregated_data
        return result


class FacilityCodeExtractor:
    """Facility code extractor"""
    
    @staticmethod
    def extract_codes(
        facilities_response: FacilityResponse,
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[str]:
        """
        Extract facility codes from response
        
        Args:
            facilities_response: FacilityResponse object
            network_filter: Network filter (optional)
            status_filter: Status filter (optional)
            
        Returns:
            List of facility codes
        """
        logger = logging.getLogger(__name__)
        
        if not facilities_response.success:
            raise ValueError(f"Failed to fetch facilities: {facilities_response.error}")
        
        facilities = facilities_response.data
        if not facilities:
            logger.warning("Facilities data is empty")
            return []
        
        # Filter facilities
        filtered = FacilityCodeExtractor.filter_facilities(
            facilities,
            network_filter=network_filter,
            status_filter=status_filter
        )
        
        # Extract codes
        codes = [f.code for f in filtered if f.code]
        
        logger.info(f"Extracted {len(codes)} facility codes")
        if network_filter:
            logger.info(f"Network filter: {network_filter}")
        if status_filter:
            logger.info(f"Status filter: {status_filter}")
        
        return codes
    
    @staticmethod
    def filter_facilities(
        facilities: List[Facility],
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[Facility]:
        """
        Filter facilities by network and unit status
        
        Args:
            facilities: List of facilities
            network_filter: Network filter (optional)
            status_filter: Status filter (optional)
            
        Returns:
            Filtered list of facilities
        """
        # Start with all facilities
        filtered = facilities
        
        # Network filtering: filter by network ID (e.g., 'NEM')
        # Only keep facilities that match the specified network
        if network_filter:
            filtered = [
                f for f in filtered
                if f.network_id == network_filter
            ]
        
        # Status filtering: filter by unit status
        # A facility matches if any of its units has the specified status
        # This checks the status_id of each unit within the facility
        if status_filter:
            result = []
            for facility in filtered:
                # Get all units for this facility (handle None case)
                units = facility.units if facility.units else []
                # Check if any unit has matching status
                # status_id is UnitStatusType enum; compare .value property
                has_matching_status = any(
                    unit.status_id.value == status_filter
                    for unit in units
                )
                # Include facility if it has at least one unit with matching status
                if has_matching_status:
                    result.append(facility)
            filtered = result
        
        return filtered


class FacilityUnitMappingManager:
    """
    Facility-unit mapping manager
    
    Manages mapping between facilities and units, used to aggregate unit-level data to facility level
    """
    
    @staticmethod
    def build_mapping_from_facilities(
        facilities_response: FacilityResponse
    ) -> Dict[str, List[str]]:
        """
        Build facility-unit mapping from FacilityResponse
        
        Args:
            facilities_response: Facility response
            
        Returns:
            facility_code -> [unit_codes] mapping dictionary
        """
        logger = logging.getLogger(__name__)
        
        if not facilities_response.success:
            raise ValueError(f"Failed to fetch facilities: {facilities_response.error}")
        
        mapping: Dict[str, List[str]] = {}
        
        for facility in facilities_response.data:
            facility_code = facility.code
            if not facility_code:
                continue
            unit_codes = [u.code for u in (facility.units or []) if u.code]
            mapping[facility_code] = unit_codes
            logger.debug(f"Facility {facility_code}: {len(unit_codes)} units")
        
        logger.info(f"Built mapping: {len(mapping)} facilities")
        return mapping
    
    @staticmethod
    def get_units_for_facility(
        mapping: Dict[str, List[str]],
        facility_code: str
    ) -> List[str]:
        """
        Get all unit codes for a facility
        
        Args:
            mapping: facility-unit mapping
            facility_code: facility code
            
        Returns:
            List of unit codes
        """
        return mapping.get(facility_code, [])
    
    @staticmethod
    def save_mapping(
        mapping: Dict[str, List[str]],
        filepath: str
    ) -> None:
        """
        Save mapping to file
        
        Args:
            mapping: facility-unit mapping
            filepath: file path
        """
        logger = logging.getLogger(__name__)
        FileManager.save_json(mapping, filepath)
        logger.info(f"Mapping saved to: {filepath}")
    
    @staticmethod
    def load_mapping(filepath: str) -> Dict[str, List[str]]:
        """
        Load mapping from file
        
        Args:
            filepath: file path
            
        Returns:
            facility-unit mapping
        """
        logger = logging.getLogger(__name__)
        mapping = FileManager.load_json(filepath)
        logger.info(f"Mapping loaded from: {filepath}")
        return mapping


class FacilityAggregator:
    """
    Facility data aggregator
    
    Aggregate unit-level time series data to facility-level time series
    """
    
    @staticmethod
    def aggregate_to_facility_level(
        timeseries_response: TimeSeriesResponse,
        facility_unit_mapping: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """
        Aggregate TimeSeriesResponse (unit-level data) to facility-level data
        
        Args:
            timeseries_response: TimeSeriesResponse object, contains unit-level data
            facility_unit_mapping: facility-unit mapping
            
        Returns:
            Aggregated facility-level data
        """
        logger = logging.getLogger(__name__)
        
        if not timeseries_response.success:
            raise ValueError(f"Failed to fetch time series data: {timeseries_response.error}")
        
        # Build reverse mapping: unit_code -> facility_code
        # This allows quick lookup of which facility a unit belongs to
        unit_to_facility: Dict[str, str] = {
            unit_code: facility_code
            for facility_code, unit_codes in facility_unit_mapping.items()
            for unit_code in unit_codes
        }
        
        # Dictionary to store aggregated data by metric
        aggregated = {}
        
        # Iterate through each metric type (e.g., power, emissions)
        for time_series_item in timeseries_response.data:
            metric = time_series_item.metric
            interval = time_series_item.interval
            unit = time_series_item.unit
            
            # Organize data by facility_code: {facility_code: {timestamp: [values]}}
            # Using defaultdict to automatically create nested dictionaries/lists
            facility_data: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
            
            # Iterate through each unit's time series result
            for result in time_series_item.results:
                unit_code = result.columns.unit_code
                if not unit_code:
                    logger.warning(f"Result {result.name} has no unit_code, skipping")
                    continue
                
                # Get facility_code using reverse mapping
                facility_code = unit_to_facility.get(unit_code)
                if not facility_code:
                    logger.warning(f"Unit {unit_code} not found in facility, skipping")
                    continue
                
                # Aggregate data from all units in the same facility
                # Collect all values for each timestamp
                for data_point in result.data:
                    timestamp = data_point.timestamp
                    value = data_point.value
                    
                    # Group by timestamp, collect values for same timestamp
                    # Multiple units may have values at the same timestamp
                    facility_data[facility_code][timestamp.isoformat()].append(value)
            
            # Sum multiple unit values for same timestamp
            # Convert from {facility_code: {timestamp: [values]}} to {facility_code: {timestamp: sum}}
            aggregated_series: Dict[str, Dict[str, float]] = {}
            for facility_code, time_values in facility_data.items():
                aggregated_series[facility_code] = {}
                for timestamp, values in time_values.items():
                    # Filter None values and sum remaining values
                    # This handles cases where some units may have missing data
                    valid_values = [v for v in values if v is not None]
                    if valid_values:
                        # Sum all unit values for this timestamp
                        aggregated_series[facility_code][timestamp] = sum(valid_values)
            
            aggregated[metric] = {
                "interval": interval,
                "unit": unit,
                "facilities": aggregated_series
            }
        
        # count facilities
        all_facilities = set()
        for metric_data in aggregated.values():
            facilities = metric_data.get("facilities", {})
            all_facilities.update(facilities.keys())
        
        logger.info(f"Successfully aggregated data: {len(aggregated)} metrics, {len(all_facilities)} facilities")
        return aggregated


class BatchHandler:
    """batch processor: process data chunks"""
    
    def __init__(self, batch_size: int, batch_delay: float):
        """
        Initialize batch processor
        
        Args:
            batch_size: batch size
            batch_delay: batch delay (seconds)
        """
        self.batch_size = batch_size
        self.batch_delay = batch_delay
    
    def create_batches(self, items: List[Any]) -> List[List[Any]]:
        """
        Create batches from list
        
        Args:
            items: list to be chunked
            
        Returns:
            List of chunks
        """
        logger = logging.getLogger(__name__)
        batches = []
        total = len(items)
        # Calculate number of batches needed (round up to ensure all items are included)
        num_batches = math.ceil(total / self.batch_size)
        
        # Split items into batches
        for i in range(num_batches):
            # Calculate start and end indices for current batch
            start = i * self.batch_size
            # Ensure end index doesn't exceed total items
            end = min(start + self.batch_size, total)
            # Extract batch slice and append to batches list
            batches.append(items[start:end])
        
        logger.info(f"Created {len(batches)} chunks (each with at most {self.batch_size} items)")
        return batches
    
    def aggregate_batches(
        self,
        batch_dir: str,
        output_file: str
    ) -> None:
        """
        Aggregate all chunk data
        
        Args:
            batch_dir: batch file directory
            output_file: output file path
        """
        logger = logging.getLogger(__name__)
        
        # Find all batch files in the directory (sorted by filename)
        # Pattern: batch_*.json (e.g., batch_001_metrics.json, batch_002_metrics.json)
        batch_files = sorted(Path(batch_dir).glob("batch_*.json"))
        if not batch_files:
            logger.warning(f"No chunk files found: {batch_dir}")
            return
        
        # Load all batch files and collect their data
        all_data = []
        for batch_file in batch_files:
            # Load JSON data from batch file
            batch_data = FileManager.load_json(str(batch_file))
            # Only include batches that have data
            if batch_data.get("data"):
                all_data.append(batch_data)
        
        # Save aggregated data to output file
        # This combines all batch files into a single JSON file
        FileManager.save_json(all_data, output_file)
        logger.info(f"Aggregated {len(all_data)} chunks to: {output_file}")


# ============================================================================
# Data Acquisition Service
# ============================================================================

class DataAcquisitionService:
    """data acquisition service: acquire data from OpenElectricity API"""
    
    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None,
        logger_instance: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize data acquisition service
        
        Args:
            api_key: OpenElectricity API key
            base_url: API base URL (optional)
            logger_instance: logger instance (optional)
        """
        self.logger = logger_instance or logging.getLogger(__name__)
        
        # Create OEClient
        if base_url:
            self.client = OEClient(api_key=api_key, base_url=base_url)
        else:
            self.client = OEClient(api_key=api_key)
    
    def _parse_data_interval(self, interval_str: str) -> DataInterval:
        """
        Parse string to DataInterval Literal type
        
        Args:
            interval_str: interval string
            
        Returns:
            DataInterval type value
        """
        valid_intervals = ['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
        
        if interval_str in valid_intervals:
            return interval_str  # type: ignore[return-value]
        else:
            self.logger.warning(f"Invalid interval value '{interval_str}', using default value '5m'")
            return '5m'  # type: ignore[return-value]
    
    def fetch_network_facilities(
        self,
        network_id: List[str] | None = None,
        status_id: List[UnitStatusType] | None = None,
        output_file: str = "data/facilities_data.json"
    ) -> FacilityResponse:
        """
        Fetch and save network facilities data
        
        Args:
            network_id: network ID list (optional)
            status_id: status ID list (optional)
            output_file: output file path
            
        Returns:
            FacilityResponse object
        """
        self.logger.info(f"Step 1: Fetch network facilities data")
        
        response = self.client.get_facilities(
            network_id=network_id,
            status_id=status_id
        )
        
        # Convert to dictionary and save
        data = DataConverter.facility_response_to_dict(response)
        data = DataConverter.add_metadata(data, "network")
        
        FileManager.save_json(data, output_file)
        self.logger.info("Network facilities data saved successfully")
        
        return response
    
    def build_facility_unit_mapping(
        self,
        facilities_response: FacilityResponse,
        mapping_file: str = "data/facility_unit_mapping.json"
    ) -> Dict[str, List[str]]:
        """
        Build facility-unit mapping
        
        Args:
            facilities_response: facility response
            mapping_file: mapping file path
            
        Returns:
            facility_code -> [unit_codes] mapping dictionary
        """
        self.logger.info("Step 1.5: Build facility-unit mapping")
        
        mapping = FacilityUnitMappingManager.build_mapping_from_facilities(facilities_response)
        
        # save mapping to file
        FacilityUnitMappingManager.save_mapping(mapping, mapping_file)
        
        return mapping
    
    def aggregate_batch_to_facility_level(
        self,
        batch_info: Dict[str, Any],
        facility_unit_mapping: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """
        Aggregate unit-level data to facility-level data
        
        Args:
            batch_info: batch data information (contains data field, data is dictionary of TimeSeriesResponse)
            facility_unit_mapping: facility-unit mapping
            
        Returns:
            Aggregated facility-level data
        """
        # parse TimeSeriesResponse in batch_info
        data_dict = batch_info.get("data", {})
        if not data_dict:
            self.logger.warning(f"No data in batch, skipping aggregation (batch_index: {batch_info.get('batch_index')})")
            return {}
        
        # build TimeSeriesResponse from dictionary
        # batch_info['data'] is result of TimeSeriesResponse.model_dump()
        try:
            timeseries_response = TimeSeriesResponse.model_validate(data_dict)
        except Exception as e:
            self.logger.error(f"Failed to parse TimeSeriesResponse: {e}", exc_info=True)
            return {}
        
        # aggregate using FacilityAggregator
        aggregated_data = FacilityAggregator.aggregate_to_facility_level(
            timeseries_response,
            facility_unit_mapping
        )
        
        return aggregated_data
    
    def merge_aggregated_batches(
        self,
        aggregated_batches: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Merge multiple batch aggregation results
        
        Args:
            aggregated_batches: aggregation results from multiple batches
            
        Returns:
            Merged facility-level data
        """
        if not aggregated_batches:
            return {}
        
        # Dictionary structure: {metric: {facility_code: {timestamp: value}}}
        # Using defaultdict to automatically create nested dictionaries
        merged: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
        
        # Merge data from all batches
        # Each batch contains aggregated facility-level data for a subset of facilities
        for batch_data in aggregated_batches:
            if not batch_data:
                continue
            
            # batch_data structure: {metric: {interval, unit, facilities: {...}}}
            # Iterate through each metric type (e.g., power, emissions)
            for metric, metric_data in batch_data.items():
                facilities = metric_data.get("facilities", {})
                
                # Merge facility data across batches
                # Same facility may appear in multiple batches (different time ranges)
                for facility_code, time_series in facilities.items():
                    # Merge time series data (if duplicate timestamps, sum them)
                    # This handles cases where same timestamp appears in multiple batches
                    for timestamp, value in time_series.items():
                        if timestamp in merged[metric][facility_code]:
                            # If duplicate timestamps exist, sum the values
                            # This can happen if batches overlap in time
                            merged[metric][facility_code][timestamp] += value
                        else:
                            # First occurrence of this timestamp for this facility
                            merged[metric][facility_code][timestamp] = value
        
        # Build final format with metadata
        # Structure: {metric: {interval, unit, facilities: {...}}}
        result = {}
        for metric, facilities_data in merged.items():
            # Get interval and unit from first batch (assume all batches have same metadata)
            if aggregated_batches and metric in aggregated_batches[0]:
                interval = aggregated_batches[0][metric].get("interval")
                unit = aggregated_batches[0][metric].get("unit")
            else:
                interval = None
                unit = None
            
            result[metric] = {
                "interval": interval,
                "unit": unit,
                "facilities": dict(facilities_data)
            }
        
        self.logger.info(f"Successfully merged {len(aggregated_batches)} batch aggregation results")
        return result
    
    def fetch_facility_metrics_batch(
        self,
        facility_codes: List[str],
        network_code: NetworkCode,
        metrics: List[DataMetric],
        interval: DataInterval,
        date_start: datetime,
        date_end: datetime,
        batch_size: int,
        batch_delay: float,
        facility_unit_mapping: Dict[str, List[str]],
        aggregate_to_facility: bool = True,
        batch_dir: str = "data/batch_data",
        aggregated_file: str = "data/facility_metrics.json"
    ) -> None:
        """
        Batch fetch and save facility metrics data, and aggregate to facility-level data
        
        Args:
            facility_codes: facility codes list
            network_code: network code
            metrics: metrics list
            interval: interval
            date_start: start time
            date_end: end time
            batch_size: batch size
            batch_delay: batch delay
            facility_unit_mapping: facility-unit mapping
            aggregate_to_facility: whether to aggregate to facility-level
            batch_dir: batch data directory
            aggregated_file: aggregated data output file
        """
        self.logger.info(f"Step 2: Batch fetch facility metrics data (total {len(facility_codes)} facilities)")
        
        # Create batch handler and split facility codes into batches
        # This prevents overwhelming the API with too many requests at once
        batch_handler = BatchHandler(batch_size, batch_delay)
        batches = batch_handler.create_batches(facility_codes)
        total_batches = len(batches)
        
        # Ensure output directory exists for batch files
        FileManager.ensure_directory(batch_dir)
        
        # Track batch processing statistics
        successful_batches = 0
        failed_batches = 0
        aggregated_batches = []  # Collect aggregation results for later merging
        
        # Process each batch of facility codes
        for idx, batch_codes in enumerate(batches):
            self.logger.info(f"Processing chunk {idx + 1}/{total_batches}: {len(batch_codes)} facilities")
            
            try:
                # Call SDK to fetch facility data for this batch
                # The API accepts multiple facility codes in a single request
                response = self.client.get_facility_data(
                    network_code=network_code,
                    facility_code=batch_codes,
                    metrics=metrics,
                    interval=interval,
                    date_start=date_start,
                    date_end=date_end
                )
                
                # Validate response before processing
                DataValidator.validate_metrics(response)
                
                # Convert response to dictionary and save original unit-level data
                # This preserves the raw data before aggregation
                data = DataConverter.timeseries_response_to_dict(response, "metrics")
                
                # Create batch filename with zero-padded index (e.g., batch_001_metrics.json)
                batch_filename = f"{batch_dir}/batch_{idx + 1:03d}_metrics.json"
                batch_info = {
                    "batch_index": idx + 1,
                    "facility_codes": batch_codes,
                    "timestamp": datetime.now().isoformat(),
                    "data": data
                }
                
                # Save batch data to file for later reference
                FileManager.save_json(batch_info, batch_filename)
                successful_batches += 1
                self.logger.info(f"Chunk {idx + 1} processed successfully")
                
                # If aggregation is enabled, aggregate current batch to facility level
                # This converts unit-level data to facility-level data
                if aggregate_to_facility:
                    try:
                        aggregated_data = self.aggregate_batch_to_facility_level(
                            batch_info,
                            facility_unit_mapping
                        )
                        if aggregated_data:
                            aggregated_batches.append(aggregated_data)
                            self.logger.info(f"Chunk {idx + 1} aggregated successfully")
                    except Exception as e:
                        self.logger.error(f"Chunk {idx + 1} aggregation failed: {e}")
            
            except Exception as e:
                failed_batches += 1
                self.logger.error(f"Chunk {idx + 1} processing failed: {e}")
            
            # batch delay (last batch does not need delay)
            if idx < total_batches - 1:
                self.logger.info(f"Waiting {batch_delay} seconds before processing next chunk...")
                time.sleep(batch_delay)
        
        self.logger.info(f"Chunk processing completed: successfully {successful_batches}, failed {failed_batches}")
        
        # If aggregation is enabled, merge all batch aggregation results and save
        if aggregate_to_facility and aggregated_batches:
            self.logger.info("Step 2.5: Merge all batch aggregation results...")
            try:
                merged_data = self.merge_aggregated_batches(aggregated_batches)
                
                if merged_data:
                    # count facilities
                    all_facilities = set()
                    for metric_data in merged_data.values():
                        facilities = metric_data.get("facilities", {})
                        all_facilities.update(facilities.keys())
                    
                    # add metadata
                    metadata = {
                        "source_batches": list(range(1, successful_batches + 1)),
                        "facility_count": len(all_facilities),
                        "date_range": {
                            "start": date_start.isoformat(),
                            "end": date_end.isoformat()
                        }
                    }
                    
                    # convert to standard format
                    formatted_data = DataConverter.aggregated_facility_data_to_dict(
                        merged_data,
                        metadata
                    )
                    
                    # Save aggregated facility-level data
                    FileManager.save_json(formatted_data, aggregated_file)
                    self.logger.info(f"Aggregated data saved to: {aggregated_file}")
                else:
                    self.logger.warning("Merged aggregated data is empty")
            except Exception as e:
                self.logger.error(f"Failed to merge aggregation results: {e}", exc_info=True)
        
        # aggregate all chunk data (original functionality remains unchanged)
        if successful_batches > 0:
            self.logger.info("Start aggregating chunk data (original unit-level data)...")
            batch_handler.aggregate_batches(
                batch_dir,
                "data/facility_units_metrics.json"
            )
        else:
            self.logger.error("No successful chunks, skipping data aggregation")
    
    def fetch_market_data(
        self,
        network_code: NetworkCode,
        metrics: List[MarketMetric],
        interval: DataInterval,
        date_start: datetime,
        date_end: datetime,
        primary_grouping: Optional[DataPrimaryGrouping] = None,
        network_region: Optional[str] = None,
        output_file: str = "data/market_data.json"
    ) -> None:
        """
        Fetch and save market data
        
        Args:
            network_code: network code
            metrics: market metrics list
            interval: interval
            date_start: start time
            date_end: end time
            primary_grouping: primary grouping (optional)
            network_region: network region (optional)
            output_file: output file path
        """
        self.logger.info(f"Step 3: Fetch market data (network_code: {network_code})")
        
        response = self.client.get_market(
            network_code=network_code,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end,
            primary_grouping=primary_grouping,
            network_region=network_region
        )
        
        # Convert to dictionary and save
        data = DataConverter.timeseries_response_to_dict(response, "market")
        data = DataConverter.add_metadata(data, "market")
        
        FileManager.save_json(data, output_file)
        self.logger.info("Market data saved successfully")
    
    def close(self) -> None:
        """close client connection"""
        self.client.close()


# ============================================================================
# Data Transformation Service
# ============================================================================

class DataTransformationService:
    """data transformation service: convert JSON data to CSV format"""
    
    def __init__(
        self,
        data_dir: str = "data",
        output_dir: str = "data/csv_output",
        logger_instance: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize converter
        
        Args:
            data_dir: JSON source file directory
            output_dir: CSV output directory
            logger_instance: logger instance (optional)
        """
        self.logger = logger_instance or logging.getLogger(__name__)
        project_root = Path(__file__).resolve().parents[2]
        self.data_dir = (project_root / data_dir).resolve()
        self.output_dir = (project_root / output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # data file paths
        self.facilities_file = self.data_dir / "facilities_data.json"
        self.metrics_file = self.data_dir / "facility_metrics.json"
        self.market_file = self.data_dir / "market_data.json"
    
    def load_json_data(self, file_path: Path) -> Dict[str, Any]:
        """
        Load JSON data file
        
        Args:
            file_path: JSON file path
            
        Returns:
            Parsed JSON data
            
        Raises:
            FileNotFoundError: file not found
            json.JSONDecodeError: JSON parse failed
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.info(f"Loaded file: {file_path}")
            return data
        except Exception as e:
            self.logger.error(f"Failed to load file {file_path}: {e}")
            raise
    
    def convert_facilities_data(self) -> None:
        """
        Convert facilities data to CSV format
        
        Convert nested JSON to two relational CSV:
        - facilities.csv: facilities master table (flattened)
        - facility_units.csv: facility units table (all unit fields)
        
        Raises:
            FileNotFoundError: JSON file not found
            KeyError: missing required fields
            json.JSONDecodeError: JSON parse failed
        """
        self.logger.info("Starting facilities conversion...")
        
        # load data
        data = self.load_json_data(self.facilities_file)
        
        # validate top-level structure
        if not isinstance(data, dict):
            error_msg = f"Invalid JSON structure: expected dict, got {type(data).__name__}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "Missing 'data' field in JSON file"
            self.logger.error(error_msg)
            raise KeyError(error_msg)
        
        facilities_data = data.get('data', [])
        
        if not isinstance(facilities_data, list):
            error_msg = f"'data' must be a list, got {type(facilities_data).__name__}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # prepare records
        facilities_records: List[Dict[str, Any]] = []
        units_records: List[Dict[str, Any]] = []
        
        for facility in facilities_data:
            # validate required facility fields
            if 'code' not in facility:
                error_msg = f"Facility record missing required field 'code': {facility}"
                self.logger.error(error_msg)
                raise KeyError(error_msg)
            
            # extract basic info (flatten 'location')
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
            
            # extract unit information (all fields)
            units = facility.get('units', [])
            if not isinstance(units, list):
                units = []
            
            facility_code = facility.get('code')
            
            for unit in units:
                if not isinstance(unit, dict):
                    self.logger.warning(f"Skip invalid unit record (facility: {facility_code}): {type(unit).__name__}")
                    continue
                
                # extract all unit fields (including date-related fields)
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
                    # start date fields
                    'commencement_date': unit.get('commencement_date'),
                    'commencement_date_specificity': unit.get('commencement_date_specificity'),
                    'commencement_date_display': unit.get('commencement_date_display'),
                    # close date fields
                    'closure_date': unit.get('closure_date'),
                    'closure_date_specificity': unit.get('closure_date_specificity'),
                    'closure_date_display': unit.get('closure_date_display'),
                    # expected operation date fields
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
                    # project approval date fields
                    'project_approval_date': unit.get('project_approval_date'),
                    'project_approval_date_specificity': unit.get('project_approval_date_specificity'),
                    'project_approval_date_display': unit.get('project_approval_date_display'),
                    # project submission date
                    'project_lodgement_date': unit.get('project_lodgement_date'),
                    # metadata timestamp
                    'created_at': unit.get('created_at'),
                    'updated_at': unit.get('updated_at')
                }
                units_records.append(unit_record)
        
        # create DataFrame and save as CSV
        facilities_df = pd.DataFrame(facilities_records)
        units_df = pd.DataFrame(units_records)
        
        # save CSV file (use NA for missing values)
        facilities_csv_path = self.output_dir / "facilities.csv"
        units_csv_path = self.output_dir / "facility_units.csv"
        
        facilities_df.to_csv(facilities_csv_path, index=False, encoding='utf-8', na_rep='NA')
        units_df.to_csv(units_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        self.logger.info(f"Facilities conversion completed: {len(facilities_records)} facilities, {len(units_records)} units")
        self.logger.info(f"Output files: {facilities_csv_path}, {units_csv_path}")
        self.logger.info(f"Facility columns: {len(facilities_df.columns)}, Unit columns: {len(units_df.columns)}")
    
    def convert_facility_metrics_data(self) -> None:
        """
        Convert facility metrics data to wide format CSV
        
        Target structure: each row contains facility_code, timestamp, power, emissions, longitude, latitude
        
        Steps:
        1. Load JSON and build long format DataFrame
        2. Pivot to wide format (power and emissions as columns)
        3. Connect facility data to add longitude/latitude
        4. Validate data integrity
        5. Save as wide format CSV
        
        Raises:
            ValueError: invalid data format
            KeyError: missing required fields
            FileNotFoundError: facility CSV not found
        """
        self.logger.info("Starting facility metrics conversion (wide format)...")
        
        # load JSON data
        data = self.load_json_data(self.metrics_file)
        
        # validate top-level structure
        if not isinstance(data, dict):
            error_msg = f"Invalid JSON structure: expected dict, got {type(data).__name__}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "Missing 'data' field in JSON file"
            self.logger.error(error_msg)
            raise KeyError(error_msg)
        
        metrics_data = data.get('data', {})
        
        if not isinstance(metrics_data, dict):
            error_msg = f"'data' field must be dict type, actual: {type(metrics_data).__name__}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # prepare data records (long format)
        data_records: List[Dict[str, Any]] = []
        
        # Iterate through each metric type (e.g., power, emissions)
        for metric_name, metric_data in metrics_data.items():
            if not isinstance(metric_data, dict):
                self.logger.warning(f"Skip invalid metric data: {metric_name} (type: {type(metric_data).__name__})")
                continue
            
            # extract facility data
            facilities = metric_data.get('facilities', {})
            
            if not isinstance(facilities, dict):
                self.logger.warning(f"Skip invalid facilities data: {metric_name} (type: {type(facilities).__name__})")
                continue
            
            # iterate through each facility time series
            for facility_code, time_series in facilities.items():
                if not isinstance(time_series, dict):
                    self.logger.warning(f"Skip invalid time series: {metric_name}/{facility_code}")
                    continue
                
                # iterate through timestamps and values
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
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # build long format DataFrame
        long_df = pd.DataFrame(data_records)
        
        self.logger.info(f"Long-form rows: {len(long_df)}")
        self.logger.info(f"Unique facilities: {long_df['facility_code'].nunique()}")
        self.logger.info(f"Unique timestamps: {long_df['timestamp'].nunique()}")
        
        # Step 1: Pivot from long format to wide format
        # Transform from: facility_code, timestamp, metric, value
        # To: facility_code, timestamp, power, emissions
        # Use 'metric' as column names, 'value' as cell values
        wide_df = long_df.pivot_table(
            index=['facility_code', 'timestamp'],  # Keep these as index
            columns='metric',  # Create columns from metric values (power, emissions)
            values='value',  # Fill cells with value column
            aggfunc='first',  # If duplicates exist, take first value
            fill_value=None  # Missing values set to None (later becomes NA in CSV)
        )
        
        # Reset index to convert facility_code and timestamp back to columns
        wide_df = wide_df.reset_index()
        
        # Remove column index name if it exists (cleanup for CSV export)
        wide_df.columns.name = None
        
        self.logger.info(f"Wide-form rows: {len(wide_df)}")
        self.logger.info(f"Wide-form columns: {list(wide_df.columns)}")
        
        # step 2: join facility data to add geolocation
        facilities_csv_path = self.output_dir / "facilities.csv"
        
        if not facilities_csv_path.exists():
            error_msg = f"Facilities CSV not found: {facilities_csv_path}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # read facility CSV
        facilities_df = pd.read_csv(facilities_csv_path)
        
        # validate required columns
        required_cols = ['code', 'lat', 'lng']
        missing_cols = [col for col in required_cols if col not in facilities_df.columns]
        if missing_cols:
            error_msg = f"Facilities CSV missing required columns: {missing_cols}"
            self.logger.error(error_msg)
            raise KeyError(error_msg)
        
        # left join: keep all metric rows, add geolocation
        wide_df = wide_df.merge(
            facilities_df[['code', 'lat', 'lng']],
            left_on='facility_code',
            right_on='code',
            how='left'
        )
        
        # step 3: rename and reorder columns
        # rename lat → latitude, lng → longitude
        wide_df = wide_df.rename(columns={'lat': 'latitude', 'lng': 'longitude'})
        
        # delete code column (already joined, no longer needed)
        if 'code' in wide_df.columns:
            wide_df = wide_df.drop(columns=['code'])
        
        # ensure column order: facility_code, timestamp, power, emissions, longitude, latitude
        # If some metrics are missing, still include the columns
        column_order = ['facility_code', 'timestamp']
        
        # add metric columns (in order: power, emissions)
        metric_order = ['power', 'emissions']
        metric_cols = [col for col in metric_order if col in wide_df.columns]
        column_order.extend(metric_cols)
        
        # add geolocation columns
        column_order.extend(['longitude', 'latitude'])
        
        # Ensure all columns exist; create as NA if missing
        for col in column_order:
            if col not in wide_df.columns:
                wide_df[col] = None
        
        # reorder columns
        wide_df = wide_df[column_order]
        
        # sort by facility_code and timestamp
        wide_df = wide_df.sort_values(['facility_code', 'timestamp'])
        
        # step 4: validate
        expected_rows = wide_df['facility_code'].nunique() * wide_df['timestamp'].nunique()
        actual_rows = len(wide_df)
        
        if actual_rows != expected_rows:
            self.logger.warning(f"Row count mismatch: expected {expected_rows}, actual {actual_rows}")
        
        # missing values
        missing_power = wide_df['power'].isna().sum()
        missing_emissions = wide_df['emissions'].isna().sum()
        missing_longitude = wide_df['longitude'].isna().sum()
        missing_latitude = wide_df['latitude'].isna().sum()
        
        self.logger.info(f"Integrity check:")
        self.logger.info(f"  - total rows: {actual_rows}")
        self.logger.info(f"  - missing power: {missing_power} ({missing_power/actual_rows*100:.2f}%)")
        self.logger.info(f"  - missing emissions: {missing_emissions} ({missing_emissions/actual_rows*100:.2f}%)")
        self.logger.info(f"  - missing longitude: {missing_longitude} ({missing_longitude/actual_rows*100:.2f}%)")
        self.logger.info(f"  - missing latitude: {missing_latitude} ({missing_latitude/actual_rows*100:.2f}%)")
        
        # unmatched facility codes (no geolocation)
        unmatched_facilities = wide_df[wide_df['longitude'].isna()]['facility_code'].unique()
        if len(unmatched_facilities) > 0:
            self.logger.warning(f"Facilities without geolocation: {len(unmatched_facilities)}")
            self.logger.warning(f"First 10 unmatched facilities: {list(unmatched_facilities[:10])}")
        
        # Step 5: save as wide format CSV
        wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
        
        wide_df.to_csv(wide_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        self.logger.info(f"Facility metrics conversion completed (wide): {len(wide_df)} rows, {len(wide_df.columns)} columns")
        self.logger.info(f"Output file: {wide_csv_path}")
        self.logger.info(f"Column order: {', '.join(wide_df.columns)}")
    
    def convert_market_data(self) -> None:
        """
        Convert market data to CSV format
        
        Raises:
            FileNotFoundError: JSON file not found
            KeyError: missing required fields
        """
        self.logger.info("Starting market data conversion...")
        
        # load data
        data = self.load_json_data(self.market_file)
        market_data = data.get('data', [])
        
        # prepare metadata and data records
        metadata_records = []
        data_records = []
        
        for metric_group in market_data:
            # extract metadata
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
            
            # extract time series data
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
        
        # create DataFrame and save as CSV
        metadata_df = pd.DataFrame(metadata_records)
        data_df = pd.DataFrame(data_records)
        
        # save CSV file
        metadata_csv_path = self.output_dir / "market_metrics_metadata.csv"
        data_csv_path = self.output_dir / "market_metrics_data.csv"
        
        metadata_df.to_csv(metadata_csv_path, index=False, encoding='utf-8')
        data_df.to_csv(data_csv_path, index=False, encoding='utf-8')
        
        self.logger.info(f"Market data conversion completed: {len(metadata_records)} metric groups, {len(data_records)} data points")
        self.logger.info(f"Output files: {metadata_csv_path}, {data_csv_path}")
    
    def validate_data_integrity(self) -> None:
        """validate data integrity"""
        self.logger.info("Starting data integrity validation...")
        
        # check if expected output files exist
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
                self.logger.info(f"{filename}: {len(df)} rows")
            else:
                self.logger.warning(f"{filename}: file not found")
        
        # validate foreign key relationships
        try:
            facilities_df = pd.read_csv(self.output_dir / "facilities.csv")
            units_df = pd.read_csv(self.output_dir / "facility_units.csv")
            
            # check facility code referential integrity
            facility_codes = set(facilities_df['code'])
            unit_facility_codes = set(units_df['facility_code'])
            
            missing_facilities = unit_facility_codes - facility_codes
            if missing_facilities:
                self.logger.warning(f"Unmatched unit references: {missing_facilities}")
            else:
                self.logger.info("Facility-unit relationship integrity check passed")
            
            # validate facility_code in facility_metrics_wide exists in facilities
            wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
            if wide_csv_path.exists():
                wide_df = pd.read_csv(wide_csv_path)
                wide_facility_codes = set(wide_df['facility_code'].unique())
                missing_in_wide = wide_facility_codes - facility_codes
                if missing_in_wide:
                    self.logger.warning(f"Unmatched facility_code in facility_metrics_wide: {len(missing_in_wide)}")
                else:
                    self.logger.info("facility_metrics_wide facility_code integrity check passed")
                
        except Exception as e:
            self.logger.error(f"Data integrity validation failed: {e}")
    
    def generate_summary_report(self) -> None:
        """generate conversion summary report"""
        self.logger.info("Generating conversion summary report...")
        
        report_path = self.output_dir / "conversion_report.txt"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("JSON to CSV Conversion Summary\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # file statistics
            csv_files = list(self.output_dir.glob("*.csv"))
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    f.write(f"{csv_file.name}: {len(df)} rows, {len(df.columns)} columns\n")
                except Exception as e:
                    f.write(f"{csv_file.name}: read failed - {e}\n")
        
        self.logger.info(f"Summary report generated: {report_path}")
    
    def run_conversion(self) -> None:
        """run complete conversion workflow"""
        self.logger.info("Starting JSON-to-CSV conversion workflow...")
        
        try:
            # convert datasets
            self.convert_facilities_data()
            self.convert_facility_metrics_data()
            self.convert_market_data()
            
            # validate integrity
            self.validate_data_integrity()
            
            # generate summary
            self.generate_summary_report()
            
            self.logger.info("Conversion workflow completed!")
            
        except Exception as e:
            self.logger.error(f"Conversion workflow failed: {e}")
            raise


# ============================================================================
# Data Publication Service
# ============================================================================

class DataPublicationService:
    """Data publication service: publish CSV data to MQTT"""
    
    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        facility_topic: str = "a02/facility_metrics/v1/stream",
        market_topic: str = "a02/market_metrics/v1/stream",
        qos: int = 0,
        logger_instance: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize data publication service
        
        Args:
            broker_host: MQTT broker host
            broker_port: MQTT broker port
            facility_topic: Facility topic
            market_topic: Market topic
            qos: MQTT QoS level
            logger_instance: Logger instance (optional)
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.facility_topic = facility_topic
        self.market_topic = market_topic
        self.qos = qos
        self.logger = logger_instance or logging.getLogger(__name__)
    
    def load_facility_data(self, csv_path: str) -> Iterator[dict]:
        """
        Load facility CSV and generate records
        
        Args:
            csv_path: CSV file path
            
        Yields:
            Each record as a dictionary containing event_time, facility_id, power, emissions
            
        Raises:
            FileNotFoundError: CSV file not found
            ValueError: Missing required columns or timestamp parsing failed
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Facility CSV not found: {csv_path}")
        
        self.logger.info(f"Loading facility data from {csv_path}")
        df = pd.read_csv(csv_path)
        
        # validate required columns
        required_cols = ["timestamp", "facility_code", "power", "emissions"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column in facility CSV: {col}")
        
        # parse timestamp
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        except Exception as e:
            raise ValueError(f"Failed to parse timestamp column in facility CSV: {e}")
        
        # sort
        df = df.sort_values(by=["timestamp", "facility_code"], kind="stable").reset_index(drop=True)
        
        self.logger.info(f"Loaded {len(df)} facility records")
        
        # generate iterator
        for _, row in df.iterrows():
            payload = {
                "event_time": row["timestamp"].isoformat(),
                "facility_id": str(row["facility_code"]),
                "power": float(row["power"]),
                "emissions": float(row["emissions"]),
            }
            
            # If geolocation exists, add it
            if "longitude" in df.columns and "latitude" in df.columns:
                if pd.notna(row["longitude"]) and pd.notna(row["latitude"]):
                    payload["longitude"] = float(row["longitude"])
                    payload["latitude"] = float(row["latitude"])
            
            yield payload
    
    def load_market_data(self, csv_path: str) -> Iterator[dict]:
        """
        Load market CSV and generate records
        
        Args:
            csv_path: CSV file path
            
        Yields:
            Each record as a dictionary containing network_code, metric, event_time, value
            
        Raises:
            FileNotFoundError: CSV file not found
            ValueError: missing required columns or timestamp parsing failed
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Market CSV not found: {csv_path}")
        
        self.logger.info(f"Loading market data from {csv_path}")
        df = pd.read_csv(csv_path)
        
        # validate required columns
        required_cols = ["network_code", "metric", "timestamp", "value"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column in market CSV: {col}")
        
        # parse timestamp
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        except Exception as e:
            raise ValueError(f"Failed to parse timestamp column in market CSV: {e}")
        
        # sort
        df = df.sort_values(by=["timestamp", "network_code", "metric"], kind="stable").reset_index(drop=True)
        
        self.logger.info(f"Loaded {len(df)} market records")
        
        # generate iterator
        for _, row in df.iterrows():
            payload = {
                "network_code": str(row["network_code"]),
                "metric": str(row["metric"]),
                "event_time": row["timestamp"].isoformat(),
                "value": float(row["value"]),
            }
            yield payload
    
    def publish_paired_streams(
        self,
        facility_iter: Iterator[dict],
        market_iter: Iterator[dict]
    ) -> None:
        """
        Publish paired streams of facility and market data to MQTT
        
        Messages are paired; wait 1 second after each pair.
        If market data runs out first, continue publishing remaining facility data.
        
        Args:
            facility_iter: facility iterator
            market_iter: market iterator
            
        Raises:
            RuntimeError: MQTT connection or publication failed
        """
        self.logger.info(
            f"Connecting to MQTT broker: {self.broker_host}:{self.broker_port}"
        )
        
        # Create MQTT client with MQTT v3.1.1 protocol
        client = mqtt.Client(protocol=mqtt.MQTTv311)
        # Connect to broker with 60 second keepalive interval
        rc = client.connect(self.broker_host, self.broker_port, keepalive=60)
        if rc != 0:
            raise RuntimeError(f"MQTT connect failed, rc={rc}")
        # Start network loop in background thread
        client.loop_start()
        self.logger.info("Connected to MQTT broker")
        
        # Track message counts for logging
        facility_count = 0
        market_count = 0
        pair_count = 0
        
        # Phase 1: Paired publishing (facility + market, zero interval)
        # Publish facility and market messages back-to-back
        self.logger.info("Starting paired publishing phase...")
        
        try:
            while True:
                # Get next facility and market records from iterators
                # Use try/except to handle iterator exhaustion gracefully
                facility_record = None
                market_record = None
                
                try:
                    facility_record = next(facility_iter)
                except StopIteration:
                    # Facility iterator exhausted
                    pass
                
                try:
                    market_record = next(market_iter)
                except StopIteration:
                    # Market iterator exhausted
                    pass
                
                # If both iterators are exhausted, exit pairing phase
                if facility_record is None and market_record is None:
                    break
                
                # If facility record exists, publish facility message
                if facility_record is not None:
                    # Serialize facility record to JSON
                    facility_payload = json.dumps(facility_record, ensure_ascii=False)
                    # Publish to facility topic
                    info = client.publish(self.facility_topic, payload=facility_payload, qos=self.qos)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
                    facility_count += 1
                    self.logger.debug(
                        f"Published facility [{facility_count}]: "
                        f"facility_id={facility_record['facility_id']} "
                        f"event_time={facility_record['event_time']}"
                    )
                
                # If market record exists, publish market message immediately (zero interval)
                # This creates a paired message pattern
                if market_record is not None:
                    market_payload = json.dumps(market_record, ensure_ascii=False)
                    info = client.publish(self.market_topic, payload=market_payload, qos=self.qos)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f"Failed to publish market message [{market_count}]: rc={info.rc}")
                    market_count += 1
                    self.logger.debug(
                        f"Published market [{market_count}]: "
                        f"network_code={market_record['network_code']} "
                        f"metric={market_record['metric']} "
                        f"event_time={market_record['event_time']} "
                        f"value={market_record['value']}"
                    )
                
                pair_count += 1
                
                # wait 1 second after each pair
                time.sleep(0.1)
                
                # log progress every 100 pairs
                if pair_count % 100 == 0:
                    self.logger.info(
                        f"Progress: {pair_count} pairs published "
                        f"({facility_count} facility, {market_count} market)"
                    )
        
        except Exception as e:
            self.logger.error(f"Error during paired publishing: {e}", exc_info=True)
            raise
        
        # Phase 2: If market data runs out first, publish remaining facility data
        remaining_facility_count = 0
        
        # If pairing loop has last facility_record, publish it
        if facility_record is not None:
            self.logger.info("Market data exhausted. Continuing with remaining facility data...")
            facility_payload = json.dumps(facility_record, ensure_ascii=False)
            info = client.publish(self.facility_topic, payload=facility_payload, qos=self.qos)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
            facility_count += 1
            remaining_facility_count += 1
            self.logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
            time.sleep(0.1)
        
        # continue publishing remaining facility data
        for facility_record in facility_iter:
            facility_payload = json.dumps(facility_record, ensure_ascii=False)
            info = client.publish(self.facility_topic, payload=facility_payload, qos=self.qos)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
            facility_count += 1
            remaining_facility_count += 1
            self.logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
            
            time.sleep(0.1)
            
            # log progress every 100 remaining facility messages
            if remaining_facility_count % 100 == 0:
                self.logger.info(
                    f"Remaining facility progress: {remaining_facility_count} published "
                    f"(total facility: {facility_count})"
                )
        
        client.loop_stop()
        client.disconnect()
        
        self.logger.info("MQTT publishing finished")
        self.logger.info(
            f"Total published: {facility_count} facility + {market_count} market = "
            f"{facility_count + market_count} messages"
        )


# ============================================================================
# Data pipeline orchestrator
# ============================================================================

class DataPipelineOrchestrator:
    """data pipeline orchestrator: coordinate data acquisition, transformation, and publication flow"""
    
    def __init__(
        self,
        config: Optional[ConfigManager] = None,
        logger_instance: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize data pipeline orchestrator
        
        Args:
            config: configuration manager instance (optional)
            logger_instance: logger instance (optional)
        """
        self.config = config or ConfigManager()
        self.logger = logger_instance or logging.getLogger(__name__)
        self.acquisition_service: Optional[DataAcquisitionService] = None
        self.transformation_service: Optional[DataTransformationService] = None
        self.publication_service: Optional[DataPublicationService] = None
    
    def run_acquisition(
        self,
        data_dir: str = "data",
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> None:
        """
        Execute data acquisition phase
        
        Args:
            data_dir: data directory
            network_filter: network filter (optional)
            status_filter: status filter (optional)
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting Data Acquisition Phase")
        self.logger.info("=" * 60)
        
        try:
            # load configuration
            api_key = self.config.load_api_key()
            base_url = self.config.load_base_url()
            date_start, date_end = self.config.load_time_window()
            interval_str = self.config.load_interval()
            batch_config = self.config.load_batch_config()
            network_filter = network_filter or self.config.load_network_filter()
            status_filter = status_filter or self.config.load_status_filter()
            
            # create acquisition service
            self.acquisition_service = DataAcquisitionService(
                api_key=api_key,
                base_url=base_url,
                logger_instance=self.logger
            )
            
            # ensure directory exists
            FileManager.ensure_directory(data_dir)
            FileManager.ensure_directory(f"{data_dir}/batch_data")
            
            # step 1: fetch network facility data
            facilities_response = self.acquisition_service.fetch_network_facilities(
                network_id=["NEM"],
                output_file=f"{data_dir}/facilities_data.json"
            )
            
            # Step 1.5: Build facility-unit mapping
            facility_unit_mapping = self.acquisition_service.build_facility_unit_mapping(
                facilities_response,
                mapping_file=f"{data_dir}/facility_unit_mapping.json"
            )
            
            # step 2: process facility metrics data (batch)
            # load facility data
            facilities_data = FileManager.load_json(f"{data_dir}/facilities_data.json")
            facility_response = FacilityResponse.model_validate(facilities_data)
            
            # extract facility codes
            facility_codes = FacilityCodeExtractor.extract_codes(
                facility_response,
                network_filter=network_filter,
                status_filter=status_filter
            )
            
            if not facility_codes:
                self.logger.warning("No facility codes extracted, using default values")
                facility_codes = ["BAYSW1", "ERARING"]
            
            # convert enum types
            network_code = "NEM"
            interval = self.acquisition_service._parse_data_interval(interval_str)
            metrics_list = [DataMetric.POWER, DataMetric.EMISSIONS]
            
            # batch processing (with aggregation)
            self.acquisition_service.fetch_facility_metrics_batch(
                facility_codes=facility_codes,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                batch_size=batch_config['batch_size'],
                batch_delay=batch_config['batch_delay'],
                facility_unit_mapping=facility_unit_mapping,
                aggregate_to_facility=True,
                batch_dir=f"{data_dir}/batch_data",
                aggregated_file=f"{data_dir}/facility_metrics.json"
            )
            
            # step 3: fetch market data
            network_code = "NEM"
            interval = self.acquisition_service._parse_data_interval(interval_str)
            metrics_list = [MarketMetric.PRICE, MarketMetric.DEMAND]
            
            self.acquisition_service.fetch_market_data(
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                primary_grouping=None,
                network_region=None,
                output_file=f"{data_dir}/market_data.json"
            )
            
            self.logger.info("Data Acquisition Phase completed successfully")
            
        except Exception as e:
            self.logger.error(f"Data Acquisition Phase failed: {e}", exc_info=True)
            raise
        finally:
            if self.acquisition_service:
                self.acquisition_service.close()
    
    def run_transformation(
        self,
        data_dir: str = "data",
        output_dir: str = "data/csv_output"
    ) -> None:
        """
        Execute data transformation phase
        
        Args:
            data_dir: JSON data directory
            output_dir: CSV output directory
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting Data Transformation Phase")
        self.logger.info("=" * 60)
        
        try:
            # create transformation service
            self.transformation_service = DataTransformationService(
                data_dir=data_dir,
                output_dir=output_dir,
                logger_instance=self.logger
            )
            
            # ensure facilities.csv exists (required for wide format conversion)
            facilities_csv_path = self.transformation_service.output_dir / "facilities.csv"
            if not facilities_csv_path.exists():
                self.logger.info("Facilities CSV missing, converting facilities first...")
                self.transformation_service.convert_facilities_data()
            
            # run complete conversion
            self.transformation_service.run_conversion()
            
            self.logger.info("Data Transformation Phase completed successfully")
            
        except Exception as e:
            self.logger.error(f"Data Transformation Phase failed: {e}", exc_info=True)
            raise
    
    def run_publication(
        self,
        facility_csv: str = "data/csv_output/facility_metrics_wide.csv",
        market_csv: str = "data/csv_output/market_metrics_data.csv"
    ) -> None:
        """
        Execute data publication phase
        
        Args:
            facility_csv: facility CSV file path
            market_csv: market CSV file path
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting Data Publication Phase")
        self.logger.info("=" * 60)
        
        try:
            # load MQTT configuration
            mqtt_config = self.config.load_mqtt_config()
            
            # create publication service
            self.publication_service = DataPublicationService(
                broker_host=mqtt_config["broker_host"],
                broker_port=mqtt_config["broker_port"],
                facility_topic=mqtt_config["facility_topic"],
                market_topic=mqtt_config["market_topic"],
                qos=mqtt_config["qos"],
                logger_instance=self.logger
            )
            
            # load data
            facility_iter = self.publication_service.load_facility_data(facility_csv)
            market_iter = self.publication_service.load_market_data(market_csv)
            
            # paired publishing
            self.publication_service.publish_paired_streams(
                facility_iter=facility_iter,
                market_iter=market_iter
            )
            
            self.logger.info("Data Publication Phase completed successfully")
            
        except Exception as e:
            self.logger.error(f"Data Publication Phase failed: {e}", exc_info=True)
            raise
    
    def run_full_pipeline(
        self,
        data_dir: str = "data",
        output_dir: str = "data/csv_output",
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> None:
        """
        Execute full data pipeline (acquisition→transformation→publication)
        
        Args:
            data_dir: data directory
            output_dir: CSV output directory
            network_filter: network filter (optional)
            status_filter: status filter (optional)
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting Full Data Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # phase 1: data acquisition
            self.run_acquisition(
                data_dir=data_dir,
                network_filter=network_filter,
                status_filter=status_filter
            )
            
            # phase 2: data transformation
            self.run_transformation(
                data_dir=data_dir,
                output_dir=output_dir
            )
            
            # phase 3: data publication
            facility_csv = f"{output_dir}/facility_metrics_wide.csv"
            market_csv = f"{output_dir}/market_metrics_data.csv"
            self.run_publication(
                facility_csv=facility_csv,
                market_csv=market_csv
            )
            
            self.logger.info("=" * 60)
            self.logger.info("Full Data Pipeline completed successfully")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Full Data Pipeline failed: {e}", exc_info=True)
            raise


# ============================================================================
# Main Function and CLI Interface
# ============================================================================

def main() -> int:
    """
    Main entry function: supports CLI parameter selection for execution mode
    
    Returns:
        Exit code (0=success, non-0=failure)
    """
    parser = argparse.ArgumentParser(
        description="Unified Data Pipeline System - integrated data acquisition, transformation and publication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Execution mode description:
  full              - execute full flow (acquisition→transformation→publication)
  acquisition       - execute acquisition only
  transformation    - execute transformation only (requires existing JSON files)
  publication       - execute publication only (requires existing CSV files)
  acquisition,transformation  - execute acquisition+transformation (no publication)acquisition+transformation (no publication)

Examples:
  # execute full flow
  python data_pipeline.py --mode full

  # acquire data only
  python data_pipeline.py --mode acquisition

  # transform data only
  python data_pipeline.py --mode transformation

  # publish data only
  python data_pipeline.py --mode publication

  # custom MQTT broker
  python data_pipeline.py --mode full --broker-host broker.hivemq.com --broker-port 1883
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        default='full',
        choices=['full', 'acquisition', 'transformation', 'publication', 'acquisition,transformation'],
        help='Execution mode (default: full)'
    )
    
    parser.add_argument(
        '--data-dir',
        type=str,
        default='data',
        help='Data directory (default: data)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/csv_output',
        help='CSV output directory (default: data/csv_output)'
    )
    
    parser.add_argument(
        '--broker-host',
        type=str,
        help='MQTT broker host (override environment variable)'
    )
    
    parser.add_argument(
        '--broker-port',
        type=int,
        help='MQTT broker port (override environment variable)'
    )
    
    parser.add_argument(
        '--facility-topic',
        type=str,
        help='Facility topic (override environment variable)'
    )
    
    parser.add_argument(
        '--market-topic',
        type=str,
        help='Market topic (override environment variable)'
    )
    
    parser.add_argument(
        '--qos',
        type=int,
        choices=[0, 1, 2],
        help='MQTT QoS level (override environment variable)'
    )
    
    parser.add_argument(
        '--network-filter',
        type=str,
        help='Network filter (override environment variable)'
    )
    
    parser.add_argument(
        '--status-filter',
        type=str,
        help='Status filter (override environment variable)'
    )
    
    args = parser.parse_args()
    
    # setup logging
    logger = LoggerConfig.setup_logger(__name__, 'logs/data_pipeline.log')
    
    try:
        logger.info("=" * 60)
        logger.info("Unified Data Pipeline System")
        logger.info("=" * 60)
        logger.info(f"Execution mode: {args.mode}")
        logger.info(f"Data directory: {args.data_dir}")
        logger.info(f"Output directory: {args.output_dir}")
        
        # create configuration manager
        config = ConfigManager()
        
        # If provided MQTT parameters, set environment variables (temporary override)
        if args.broker_host:
            os.environ["MQTT_BROKER_HOST"] = args.broker_host
        if args.broker_port:
            os.environ["MQTT_BROKER_PORT"] = str(args.broker_port)
        if args.facility_topic:
            os.environ["MQTT_FACILITY_TOPIC"] = args.facility_topic
        if args.market_topic:
            os.environ["MQTT_MARKET_TOPIC"] = args.market_topic
        if args.qos is not None:
            os.environ["MQTT_QOS"] = str(args.qos)
        
        # create orchestrator
        orchestrator = DataPipelineOrchestrator(config=config, logger_instance=logger)
        
        # execute based on mode
        if args.mode == 'full':
            orchestrator.run_full_pipeline(
                data_dir=args.data_dir,
                output_dir=args.output_dir,
                network_filter=args.network_filter,
                status_filter=args.status_filter
            )
        
        elif args.mode == 'acquisition':
            orchestrator.run_acquisition(
                data_dir=args.data_dir,
                network_filter=args.network_filter,
                status_filter=args.status_filter
            )
        
        elif args.mode == 'transformation':
            orchestrator.run_transformation(
                data_dir=args.data_dir,
                output_dir=args.output_dir
            )
        
        elif args.mode == 'publication':
            facility_csv = f"{args.output_dir}/facility_metrics_wide.csv"
            market_csv = f"{args.output_dir}/market_metrics_data.csv"
            orchestrator.run_publication(
                facility_csv=facility_csv,
                market_csv=market_csv
            )
        
        elif args.mode == 'acquisition,transformation':
            orchestrator.run_acquisition(
                data_dir=args.data_dir,
                network_filter=args.network_filter,
                status_filter=args.status_filter
            )
            orchestrator.run_transformation(
                data_dir=args.data_dir,
                output_dir=args.output_dir
            )
        
        else:
            logger.error(f"Unknown mode: {args.mode}")
            return 1
        
        logger.info("=" * 60)
        logger.info("Pipeline execution completed successfully")
        logger.info("=" * 60)
        return 0
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"Error: file not found - {e}")
        return 1
    
    except ValueError as e:
        logger.error(f"Invalid data format: {e}")
        print(f"Error: invalid data format - {e}")
        return 1
    
    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        print(f"Error: runtime error - {e}")
        return 1
    
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


# initialize global logger
logger = LoggerConfig.setup_logger(__name__, 'logs/data_pipeline.log')

