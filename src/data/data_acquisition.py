#!/usr/bin/env python3
"""
Data acquisition program v2 based on OpenElectricity SDK (OEClient)
Approach A-2: client-side aggregation + facility-unit mapping to produce facility-level data

Features:
- Fetch network facilities (Network)
- Batch fetch facility metrics (Metrics; supports batching)
- Aggregate unit-level data to facility-level
- Fetch market data (Market)
"""

import os
import json
import time
import math
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

# Environment variables are provided externally (via ConfigLoader); do not hard-code API keys in code
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

# SDK imports
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

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/data_acquisition.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """Configuration loader: reads settings from environment variables"""
    
    @staticmethod
    def load_api_key() -> str:
        """Load API key (required)"""
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            raise ValueError("OPENELECTRICITY_API_KEY is not set")
        return api_key
    
    @staticmethod
    def load_base_url() -> Optional[str]:
        """Load API base URL (optional)"""
        return os.getenv("OE_BASE_URL")
    
    @staticmethod
    def load_time_window() -> Tuple[datetime, datetime]:
        """Load time window (optional, defaults)"""
        
        date_start = datetime(2025, 10, 1)
        date_end = datetime(2025, 10, 8)
        
        return date_start, date_end
    
    @staticmethod
    def load_interval() -> str:
        """Load data interval (optional, default '5m')"""
        return os.getenv("INTERVAL", "5m")
    
    @staticmethod
    def load_batch_config() -> Dict[str, Any]:
        """Load batch processing config"""
        batch_size = int(os.getenv("BATCH_SIZE", "30"))
        batch_delay = float(os.getenv("BATCH_DELAY_SECS", "0.5"))
        return {
            "batch_size": batch_size,
            "batch_delay": batch_delay
        }
    
    @staticmethod
    def load_network_filter() -> Optional[str]:
        """Load network filter (optional, default 'NEM')"""
        return os.getenv("NETWORK_FILTER", "NEM")
    
    @staticmethod
    def load_status_filter() -> Optional[str]:
        """Load status filter (optional, default 'operating')"""
        return os.getenv("STATUS_FILTER", "operating")


class DataConverter:
    """Data converter: transform Pydantic models to dictionaries"""
    
    @staticmethod
    def facility_response_to_dict(response: FacilityResponse) -> Dict[str, Any]:
        """Convert FacilityResponse to dict"""
        return response.model_dump()
    
    @staticmethod
    def timeseries_response_to_dict(
        response: TimeSeriesResponse,
        data_type: str = "metrics"
    ) -> Dict[str, Any]:
        """Convert TimeSeriesResponse to dict"""
        data = response.model_dump()
        data['data_type'] = data_type
        return data
    
    @staticmethod
    def add_metadata(
        data: Dict[str, Any],
        data_type: str
    ) -> Dict[str, Any]:
        """Add processing metadata"""
        data['processed_at'] = datetime.now().isoformat()
        if 'data_type' not in data:
            data['data_type'] = data_type
        if 'total_records' not in data:
            data_list = data.get('data', [])
            if isinstance(data_list, list):
                data['total_records'] = len(data_list)
        return data
    
    @staticmethod
    def aggregated_facility_data_to_dict(
        aggregated_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Convert aggregated facility-level data to a dictionary.
        
        Args:
            aggregated_data: Aggregated facility-level data
            metadata: Additional metadata
            
        Returns:
            Formatted dict payload
        """
        result = {
            "version": "1.0",
            "aggregated_at": datetime.now().isoformat(),
            "aggregation_method": "facility_unit_mapping",
        }
        
        # Add extra metadata
        if metadata:
            result.update(metadata)
        
        # Attach aggregated data
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
        """Extract facility codes from response"""
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
        """Filter facilities by network and unit status"""
        filtered = facilities
        
        # Network filter
        if network_filter:
            filtered = [
                f for f in filtered
                if f.network_id == network_filter
            ]
        
        # Status filter (check units' status_id)
        if status_filter:
            result = []
            for facility in filtered:
                units = facility.units if facility.units else []
                # status_id is UnitStatusType enum; compare .value
                has_matching_status = any(
                    unit.status_id.value == status_filter
                    for unit in units
                )
                if has_matching_status:
                    result.append(facility)
            filtered = result
        
        return filtered


class FacilityUnitMappingManager:
    """
    Facility-Unit mapping manager.
    
    Manages mapping between facilities and units for aggregating unit-level data to facility-level.
    """
    
    @staticmethod
    def build_mapping_from_facilities(
        facilities_response: FacilityResponse
    ) -> Dict[str, List[str]]:
        """
        Build facility-unit mapping from FacilityResponse.
        
        Args:
            facilities_response: Facilities response
            
        Returns:
            facility_code -> [unit_codes] mapping
        """
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
        Get all unit codes for a facility.
        
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
        Save mapping to file.
        
        Args:
            mapping: facility-unit mapping
            filepath: file path
        """
        FileManager.save_json(mapping, filepath)
        logger.info(f"Mapping saved to: {filepath}")
    
    @staticmethod
    def load_mapping(filepath: str) -> Dict[str, List[str]]:
        """
        Load mapping from file.
        
        Args:
            filepath: file path
            
        Returns:
            facility-unit mapping
        """
        mapping = FileManager.load_json(filepath)
        logger.info(f"Mapping loaded from: {filepath}")
        return mapping


class FacilityAggregator:
    """
    Facility data aggregator.
    
    Aggregates unit-level time series data into facility-level time series.
    """
    
    @staticmethod
    def aggregate_to_facility_level(
        timeseries_response: TimeSeriesResponse,
        facility_unit_mapping: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """
        Aggregate TimeSeriesResponse (unit-level data) to facility-level data.
        
        Args:
            timeseries_response: TimeSeriesResponse object, containing unit-level data
            facility_unit_mapping: facility-unit mapping
            
        Returns:
            Aggregated facility-level data
        """
        if not timeseries_response.success:
            raise ValueError(f"Failed to fetch time series data: {timeseries_response.error}")
        
        # Build reverse mapping: unit_code -> facility_code
        unit_to_facility: Dict[str, str] = {
            unit_code: facility_code
            for facility_code, unit_codes in facility_unit_mapping.items()
            for unit_code in unit_codes
        }
        
        aggregated = {}
        
        # Iterate over each metric
        for time_series_item in timeseries_response.data:
            metric = time_series_item.metric
            interval = time_series_item.interval
            unit = time_series_item.unit
            
            # Organize data by facility_code
            facility_data: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
            
            # Iterate over each unit's result
            for result in time_series_item.results:
                unit_code = result.columns.unit_code
                if not unit_code:
                    logger.warning(f"Result {result.name} has no unit_code, skipping")
                    continue
                
                # Use mapping to get facility_code
                facility_code = unit_to_facility.get(unit_code)
                if not facility_code:
                    logger.warning(f"Unit {unit_code} not found in facility, skipping")
                    continue
                
                # Aggregate data for all units in the same facility
                for data_point in result.data:
                    timestamp = data_point.timestamp
                    value = data_point.value
                    
                    # Group by timestamp and sum values for the same timestamp
                    facility_data[facility_code][timestamp.isoformat()].append(value)
            
            # Sum values for multiple units at the same timestamp
            aggregated_series: Dict[str, Dict[str, float]] = {}
            for facility_code, time_values in facility_data.items():
                aggregated_series[facility_code] = {}
                for timestamp, values in time_values.items():
                    # Filter None values and sum them
                    valid_values = [v for v in values if v is not None]
                    if valid_values:
                        aggregated_series[facility_code][timestamp] = sum(valid_values)
            
            aggregated[metric] = {
                "interval": interval,
                "unit": unit,
                "facilities": aggregated_series
            }
        
        # Count number of facilities
        all_facilities = set()
        for metric_data in aggregated.values():
            facilities = metric_data.get("facilities", {})
            all_facilities.update(facilities.keys())
        
        logger.info(f"Successfully aggregated data: {len(aggregated)} metrics, {len(all_facilities)} facilities")
        return aggregated


class BatchHandler:
    """Batch processor, handling data chunks"""
    
    def __init__(self, batch_size: int, batch_delay: float):
        self.batch_size = batch_size
        self.batch_delay = batch_delay
    
    def create_batches(self, items: List[Any]) -> List[List[Any]]:
        """Chunk list"""
        batches = []
        total = len(items)
        num_batches = math.ceil(total / self.batch_size)
        
        for i in range(num_batches):
            start = i * self.batch_size
            end = min(start + self.batch_size, total)
            batches.append(items[start:end])
        
        logger.info(f"Created {len(batches)} chunks (each with at most {self.batch_size} items)")
        return batches
    
    def aggregate_batches(
        self,
        batch_dir: str,
        output_file: str
    ) -> None:
        """Aggregate all chunk data"""
        batch_files = sorted(Path(batch_dir).glob("batch_*.json"))
        if not batch_files:
            logger.warning(f"No chunk files found: {batch_dir}")
            return
        
        all_data = []
        for batch_file in batch_files:
            batch_data = FileManager.load_json(str(batch_file))
            if batch_data.get("data"):
                all_data.append(batch_data)
        
        FileManager.save_json(all_data, output_file)
        logger.info(f"Aggregated {len(all_data)} chunks to: {output_file}")


class DataValidator:
    """Data validator"""
    
    @staticmethod
    def validate_metrics(response: TimeSeriesResponse) -> None:
        """Validate metrics data response"""
        if not response.success:
            raise ValueError(f"Failed to fetch metrics data: {response.error}")
        
        if not response.data:
            raise ValueError("Metrics data is empty")
        
        logger.debug(f"Validation passed: {len(response.data)} metrics")


class FileManager:
    """File manager"""
    
    @staticmethod
    def save_json(data: Dict[str, Any] | List[Dict[str, Any]], filepath: str) -> None:
        """Save JSON data to file"""
        FileManager.ensure_directory(os.path.dirname(filepath))
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.debug(f"Data saved to: {filepath}")
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any] | List[Dict[str, Any]]:
        """Load JSON data from file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def ensure_directory(dirpath: str) -> None:
        """Ensure directory exists"""
        if dirpath:
            Path(dirpath).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def save_aggregated_data(
        data: Dict[str, Any],
        filepath: str
    ) -> None:
        """
        Save aggregated facility-level data
        
        Args:
            data: Aggregated facility-level data
            filepath: File path
        """
        FileManager.save_json(data, filepath)
        logger.info(f"Aggregated data saved to: {filepath}")


def _parse_data_interval(interval_str: str) -> DataInterval:
    """Parse string to DataInterval Literal type"""
    valid_intervals = ['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
    
    if interval_str in valid_intervals:
        return interval_str  # type: ignore[return-value]
    else:
        # If not in valid values, use default value '5m'
        logger.warning(f"Invalid interval value '{interval_str}', using default value '5m'")
        return '5m'  # type: ignore[return-value]


def fetch_and_save_network_data(
    client: OEClient,
    network_id: List[str] | None = None,
    status_id: List[UnitStatusType] | None = None,
    output_file: str = "data/facilities_data.json"
) -> FacilityResponse:
    """Fetch and save network facilities data"""
    logger.info(f"Step 1: Fetch network facilities data")
    
    response = client.get_facilities(
        network_id=network_id,
        status_id=status_id
    )
    
    # Convert to dictionary and save
    data = DataConverter.facility_response_to_dict(response)
    data = DataConverter.add_metadata(data, "network")
    
    FileManager.save_json(data, output_file)
    logger.info("Network facilities data saved successfully")
    
    return response


def build_facility_unit_mapping(
    client: OEClient,
    facilities_response: FacilityResponse
) -> Dict[str, List[str]]:
    """
    Build facility-unit mapping
    
    Args:
        client: OEClient instance
        facilities_response: Facilities response
        
    Returns:
        facility_code -> [unit_codes] mapping
    """
    logger.info("Step 1.5: Build facility-unit mapping")
    
    mapping = FacilityUnitMappingManager.build_mapping_from_facilities(facilities_response)
    
    # Save mapping to file
    mapping_file = "data/facility_unit_mapping.json"
    FacilityUnitMappingManager.save_mapping(mapping, mapping_file)
    
    return mapping


def aggregate_batch_to_facility_level(
    batch_info: Dict[str, Any],
    facility_unit_mapping: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    Aggregate unit-level data to facility-level data
    
    Args:
        batch_info: Batch data information (contains data field, data is a dictionary of TimeSeriesResponse)
        facility_unit_mapping: facility-unit mapping
        
    Returns:
        Aggregated facility-level data
    """
    # Parse TimeSeriesResponse in batch_info
    data_dict = batch_info.get("data", {})
    if not data_dict:
        logger.warning(f"No data in batch, skipping aggregation (batch_index: {batch_info.get('batch_index')})")
        return {}
    
    # Build TimeSeriesResponse from dictionary
    # batch_info['data'] is the result of TimeSeriesResponse.model_dump()
    try:
        timeseries_response = TimeSeriesResponse.model_validate(data_dict)
    except Exception as e:
        logger.error(f"Failed to parse TimeSeriesResponse: {e}", exc_info=True)
        return {}
    
    # Use FacilityAggregator to aggregate
    aggregated_data = FacilityAggregator.aggregate_to_facility_level(
        timeseries_response,
        facility_unit_mapping
    )
    
    return aggregated_data


def merge_aggregated_batches(
    aggregated_batches: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Merge multiple batch aggregation results
    
    Args:
        aggregated_batches: Aggregation results from multiple batches
        
    Returns:
        Merged facility-level data
    """
    if not aggregated_batches:
        return {}
    
    merged: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    
    # Merge data from all batches
    for batch_data in aggregated_batches:
        if not batch_data:
            continue
        
        # batch_data is {metric: {interval, unit, facilities: {...}}}
        for metric, metric_data in batch_data.items():
            facilities = metric_data.get("facilities", {})
            
            # Merge facility data
            for facility_code, time_series in facilities.items():
                # Merge time series data (sum if duplicate timestamps)
                for timestamp, value in time_series.items():
                    if timestamp in merged[metric][facility_code]:
                        # Sum if duplicate timestamps
                        merged[metric][facility_code][timestamp] += value
                    else:
                        merged[metric][facility_code][timestamp] = value
    
    # Build final format
    result = {}
    for metric, facilities_data in merged.items():
        # Get interval and unit from first batch (assuming all batches are the same)
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
    
    logger.info(f"Successfully merged {len(aggregated_batches)} batch aggregation results")
    return result


def fetch_and_save_metrics_batch(
    client: OEClient,
    facility_codes: List[str],
    network_code: NetworkCode,
    metrics: List[DataMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    batch_size: int,
    batch_delay: float,
    facility_unit_mapping: Dict[str, List[str]],
    aggregate_to_facility: bool = True
) -> None:
    """
    Batch fetch and save facility metrics data, and aggregate to facility-level data
    
    Args:
        client: OEClient instance
        facility_codes: Facility codes list
        network_code: Network code
        metrics: Metrics list
        interval: Time interval
        date_start: Start time
        date_end: End time
        batch_size: Batch size
        batch_delay: Batch delay
        facility_unit_mapping: facility-unit mapping
        aggregate_to_facility: Whether to aggregate to facility-level
    """
    logger.info(f"Step 2: Batch fetch facility metrics data (total {len(facility_codes)} facilities)")
    
    batch_handler = BatchHandler(batch_size, batch_delay)
    batches = batch_handler.create_batches(facility_codes)
    total_batches = len(batches)
    
    # Ensure output directory exists
    FileManager.ensure_directory("data/batch_data")
    
    successful_batches = 0
    failed_batches = 0
    aggregated_batches = []  # For collecting aggregation results
    
    for idx, batch_codes in enumerate(batches):
        logger.info(f"Processing chunk {idx + 1}/{total_batches}: {len(batch_codes)} facilities")
        
        try:
            # Call SDK to fetch data
            response = client.get_facility_data(
                network_code=network_code,
                facility_code=batch_codes,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
            
            DataValidator.validate_metrics(response)
            
            # Convert to dictionary and save original unit-level data
            data = DataConverter.timeseries_response_to_dict(response, "metrics")
            
            batch_filename = f"data/batch_data/batch_{idx + 1:03d}_metrics.json"
            batch_info = {
                "batch_index": idx + 1,
                "facility_codes": batch_codes,
                "timestamp": datetime.now().isoformat(),
                "data": data
            }
            
            FileManager.save_json(batch_info, batch_filename)
            successful_batches += 1
            logger.info(f"Chunk {idx + 1} processed successfully")
            
            # If aggregation is enabled, aggregate current batch
            if aggregate_to_facility:
                try:
                    aggregated_data = aggregate_batch_to_facility_level(
                        batch_info,
                        facility_unit_mapping
                    )
                    if aggregated_data:
                        aggregated_batches.append(aggregated_data)
                        logger.info(f"Chunk {idx + 1} aggregated successfully")
                except Exception as e:
                    logger.error(f"Chunk {idx + 1} aggregation failed: {e}")
            
        except Exception as e:
            failed_batches += 1
            logger.error(f"Chunk {idx + 1} processing failed: {e}")
        
        # Batch delay (last chunk does not need delay)
        if idx < total_batches - 1:
            logger.info(f"Waiting {batch_delay} seconds before processing next chunk...")
            time.sleep(batch_delay)
    
    logger.info(f"Chunk processing completed: successfully {successful_batches}, failed {failed_batches}")
    
    # If aggregation is enabled, merge all batch aggregation results and save
    if aggregate_to_facility and aggregated_batches:
        logger.info("Step 2.5: Merge all batch aggregation results...")
        try:
            merged_data = merge_aggregated_batches(aggregated_batches)
            
            if merged_data:
                # Count number of facilities
                all_facilities = set()
                for metric_data in merged_data.values():
                    facilities = metric_data.get("facilities", {})
                    all_facilities.update(facilities.keys())
                
                # Add metadata
                metadata = {
                    "source_batches": list(range(1, successful_batches + 1)),
                    "facility_count": len(all_facilities),
                    "date_range": {
                        "start": date_start.isoformat(),
                        "end": date_end.isoformat()
                    }
                }
                
                # Convert to standard format
                formatted_data = DataConverter.aggregated_facility_data_to_dict(
                    merged_data,
                    metadata
                )
                
                # Save aggregated facility-level data
                aggregated_file = "data/facility_metrics.json"
                FileManager.save_aggregated_data(formatted_data, aggregated_file)
                logger.info(f"Aggregated data saved to: {aggregated_file}")
            else:
                logger.warning("Merged aggregated data is empty")
        except Exception as e:
            logger.error(f"Failed to merge aggregation results: {e}", exc_info=True)
    
    # Aggregate all chunk data (original functionality remains unchanged)
    if successful_batches > 0:
        logger.info("Start aggregating chunk data (original unit-level data)...")
        batch_handler.aggregate_batches(
            "data/batch_data",
            "data/facility_units_metrics.json"
        )
    else:
        logger.error("No successful chunks, skipping data aggregation")


def fetch_and_save_market_data(
    client: OEClient,
    network_code: NetworkCode,
    metrics: List[MarketMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    primary_grouping: Optional[DataPrimaryGrouping] = None,
    network_region: Optional[str] = None,
    output_file: str = "data/market_data.json"
) -> None:
    """Fetch and save market data"""
    logger.info(f"Step 3: Fetch market data (network_code: {network_code})")
    
    response = client.get_market(
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
    logger.info("Market data saved successfully")


def main() -> int:
    """Main function"""
    try:
        # Load configuration
        api_key = ConfigLoader.load_api_key()
        base_url = ConfigLoader.load_base_url()
        date_start, date_end = ConfigLoader.load_time_window()
        interval_str = ConfigLoader.load_interval()
        batch_config = ConfigLoader.load_batch_config()
        network_filter = ConfigLoader.load_network_filter()
        status_filter = ConfigLoader.load_status_filter()
        
        # Create client
        if base_url:
            client = OEClient(api_key=api_key, base_url=base_url)
        else:
            client = OEClient(api_key=api_key)
        
        # Ensure directory exists
        FileManager.ensure_directory("data")
        FileManager.ensure_directory("data/batch_data")
        
        success_count = 0
        total_tasks = 3
        
        # Step 1: Fetch network facilities data
        try:
            facilities_response = fetch_and_save_network_data(
                client=client,
                network_id=["NEM"],
                output_file="data/facilities_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Step 1 failed: {e}", exc_info=True)
            facilities_response = None
        
        # Step 1.5: Build facility-unit mapping
        facility_unit_mapping = {}
        if facilities_response:
            try:
                facility_unit_mapping = build_facility_unit_mapping(
                    client=client,
                    facilities_response=facilities_response
                )
                logger.info("Step 1.5 completed: mapping building successful")
            except Exception as e:
                logger.error(f"Step 1.5 failed: {e}", exc_info=True)
                logger.warning("Will continue using empty mapping, aggregation functionality may be inaccurate")
        
        # Step 2: Process facility metrics data (batch)
        try:
            # Load facilities data
            facilities_data = FileManager.load_json("data/facilities_data.json")
            facility_response = FacilityResponse.model_validate(facilities_data)
            
            # Extract facility codes
            facility_codes = FacilityCodeExtractor.extract_codes(
                facility_response,
                network_filter=network_filter,
                status_filter=status_filter
            )
            
            if not facility_codes:
                logger.warning("No facility codes extracted, using default values")
                facility_codes = ["BAYSW1", "ERARING"]
            
            # Convert enum types
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [DataMetric.POWER, DataMetric.EMISSIONS]
            
            # Batch processing (with aggregation)
            fetch_and_save_metrics_batch(
                client=client,
                facility_codes=facility_codes,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                batch_size=batch_config['batch_size'],
                batch_delay=batch_config['batch_delay'],
                facility_unit_mapping=facility_unit_mapping,
                aggregate_to_facility=True  # Enable aggregation
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Step 2 failed: {e}", exc_info=True)
        
        # Step 3: Fetch market data
        try:
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [MarketMetric.PRICE, MarketMetric.DEMAND]
            
            fetch_and_save_market_data(
                client=client,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                primary_grouping=None,
                network_region=None,
                output_file="data/market_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Step 3 failed: {e}", exc_info=True)
        
        # Result summary
        logger.info("=" * 60)
        logger.info("Data acquisition process completed")
        logger.info(f"Successfully completed tasks: {success_count}/{total_tasks}")
        logger.info(f"Processing time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return 0 if success_count == total_tasks else 1
        
    except Exception as e:
        logger.error(f"Program execution failed: {e}", exc_info=True)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)

