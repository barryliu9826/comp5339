#!/usr/bin/env python3
"""
MQTT Publisher
Purpose: publish facility_metrics_wide.csv and market_metrics_data.csv in pairs to MQTT.
Constraints: stateless; no retries/fallbacks; fail fast on error.
"""

from __future__ import annotations

import json
import os
import time
import argparse
import logging
from typing import Iterator
import pandas as pd
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Defaults
DEFAULT_FACILITY_CSV = "data/csv_output/facility_metrics_wide.csv"
DEFAULT_MARKET_CSV = "data/csv_output/market_metrics_data.csv"
DEFAULT_FACILITY_TOPIC = "a02/facility_metrics/v1/stream"
DEFAULT_MARKET_TOPIC = "a02/market_metrics/v1/stream"


def load_facility_data(csv_path: str) -> Iterator[dict]:
    """
    Load facility CSV and yield records.

    Args:
        csv_path: Path to CSV file

    Yields:
        Each record as a dict with event_time, facility_id, power, emissions

    Raises:
        FileNotFoundError: CSV file not found
        ValueError: Required columns missing or timestamp parse failed
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Facility CSV not found: {csv_path}")
    
    logger.info(f"Loading facility data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Validate required columns
    required_cols = ["timestamp", "facility_code", "power", "emissions"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in facility CSV: {col}")
    
    # Parse timestamp
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise ValueError(f"Failed to parse timestamp column in facility CSV: {e}")
    
    # Sort
    df = df.sort_values(by=["timestamp", "facility_code"], kind="stable").reset_index(drop=True)
    
    logger.info(f"Loaded {len(df)} facility records")
    
    # Yield as iterator
    for _, row in df.iterrows():
        payload = {
            "event_time": row["timestamp"].isoformat(),
            "facility_id": str(row["facility_code"]),
            "power": float(row["power"]),
            "emissions": float(row["emissions"]),
        }
        
        # Add geolocation if present
        if "longitude" in df.columns and "latitude" in df.columns:
            if pd.notna(row["longitude"]) and pd.notna(row["latitude"]):
                payload["longitude"] = float(row["longitude"])
                payload["latitude"] = float(row["latitude"])
        
        yield payload


def load_market_data(csv_path: str) -> Iterator[dict]:
    """
    Load market CSV and yield records.

    Args:
        csv_path: Path to CSV file

    Yields:
        Each record as a dict with network_code, metric, event_time, value

    Raises:
        FileNotFoundError: CSV file not found
        ValueError: Required columns missing or timestamp parse failed
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Market CSV not found: {csv_path}")
    
    logger.info(f"Loading market data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Validate required columns
    required_cols = ["network_code", "metric", "timestamp", "value"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in market CSV: {col}")
    
    # Parse timestamp
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise ValueError(f"Failed to parse timestamp column in market CSV: {e}")
    
    # Sort
    df = df.sort_values(by=["timestamp", "network_code", "metric"], kind="stable").reset_index(drop=True)
    
    logger.info(f"Loaded {len(df)} market records")
    
    # Yield as iterator
    for _, row in df.iterrows():
        payload = {
            "network_code": str(row["network_code"]),
            "metric": str(row["metric"]),
            "event_time": row["timestamp"].isoformat(),
            "value": float(row["value"]),
        }
        yield payload


def publish_paired_streams(
    facility_iter: Iterator[dict],
    market_iter: Iterator[dict],
    broker_host: str,
    broker_port: int,
    facility_topic: str,
    market_topic: str,
    qos: int,
) -> None:
    """
    Publish facility and market data to MQTT in pairs.

    Messages are back-to-back; wait 1 second after each pair.
    If market data exhausts first, continue publishing remaining facility data.

    Args:
        facility_iter: Facility iterator
        market_iter: Market iterator
        broker_host: MQTT broker host
        broker_port: MQTT broker port
        facility_topic: Facility topic
        market_topic: Market topic
        qos: MQTT QoS level
    Raises:
        RuntimeError: MQTT connection or publish failed
    """
    logger.info(
        f"Connecting to MQTT broker: {broker_host}:{broker_port}"
    )
    
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    rc = client.connect(broker_host, broker_port, keepalive=60)
    if rc != 0:
        raise RuntimeError(f"MQTT connect failed, rc={rc}")
    client.loop_start()
    logger.info("Connected to MQTT broker")
    
    facility_count = 0
    market_count = 0
    pair_count = 0
    
    # Phase 1: paired publishing (facility + market, zero gap)
    logger.info("Starting paired publishing phase...")
    
    try:
        while True:
            # Fetch next facility and market records
            facility_record = None
            market_record = None
            
            try:
                facility_record = next(facility_iter)
            except StopIteration:
                pass
            
            try:
                market_record = next(market_iter)
            except StopIteration:
                pass
            
            # If both are None, exit paired phase
            if facility_record is None and market_record is None:
                break
            
            # Publish facility message if present
            if facility_record is not None:
                facility_payload = json.dumps(facility_record, ensure_ascii=False)
                info = client.publish(facility_topic, payload=facility_payload, qos=qos)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
                facility_count += 1
                logger.debug(
                    f"Published facility [{facility_count}]: "
                    f"facility_id={facility_record['facility_id']} "
                    f"event_time={facility_record['event_time']}"
                )
            
            # Immediately publish market message if present (zero gap)
            if market_record is not None:
                market_payload = json.dumps(market_record, ensure_ascii=False)
                info = client.publish(market_topic, payload=market_payload, qos=qos)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"Failed to publish market message [{market_count}]: rc={info.rc}")
                market_count += 1
                logger.debug(
                    f"Published market [{market_count}]: "
                    f"network_code={market_record['network_code']} "
                    f"metric={market_record['metric']} "
                    f"event_time={market_record['event_time']} "
                    f"value={market_record['value']}"
                )
            
            pair_count += 1
            
            # Wait 1s after each pair
            time.sleep(1.0)
            
            # Log progress every 100 pairs
            if pair_count % 100 == 0:
                logger.info(
                    f"Progress: {pair_count} pairs published "
                    f"({facility_count} facility, {market_count} market)"
                )
    
    except Exception as e:
        logger.error(f"Error during paired publishing: {e}", exc_info=True)
        raise
    
    # Phase 2: publish remaining facility data if market exhausted first
    remaining_facility_count = 0
    
    # If a last facility_record remains from pairing loop, publish it
    if facility_record is not None:
        logger.info("Market data exhausted. Continuing with remaining facility data...")
        facility_payload = json.dumps(facility_record, ensure_ascii=False)
        info = client.publish(facility_topic, payload=facility_payload, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
        facility_count += 1
        remaining_facility_count += 1
        logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
        time.sleep(1.0)
    
    # Continue publishing remaining facility data
    for facility_record in facility_iter:
        facility_payload = json.dumps(facility_record, ensure_ascii=False)
        info = client.publish(facility_topic, payload=facility_payload, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
        facility_count += 1
        remaining_facility_count += 1
        logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
        
        time.sleep(1.0)
        
        # Log progress every 100 remaining facility messages
        if remaining_facility_count % 100 == 0:
            logger.info(
                f"Remaining facility progress: {remaining_facility_count} published "
                f"(total facility: {facility_count})"
            )
    
    client.loop_stop()
    client.disconnect()
    
    logger.info("MQTT publishing finished")
    logger.info(
        f"Total published: {facility_count} facility + {market_count} market = "
        f"{facility_count + market_count} messages"
    )


def main() -> int:
    """
    Entrypoint: load and publish facility and market CSV data to MQTT in pairs.

    Returns:
        Exit code (0 = success, non-zero = failure)
    """
    parser = argparse.ArgumentParser(
        description="Publish facility and market CSV to MQTT in pairs (1s interval)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults
  python mqtt_publisher.py

  # Specify broker
  python mqtt_publisher.py --broker-host broker.hivemq.com --broker-port 1883
        """
    )
    parser.add_argument(
        "--facility-csv",
        type=str,
        default=DEFAULT_FACILITY_CSV,
        help=f"Path to facility CSV (default: {DEFAULT_FACILITY_CSV})"
    )
    parser.add_argument(
        "--market-csv",
        type=str,
        default=DEFAULT_MARKET_CSV,
        help=f"Path to market CSV (default: {DEFAULT_MARKET_CSV})"
    )
    parser.add_argument(
        "--broker-host",
        type=str,
        default="broker.hivemq.com",
        help="MQTT broker host (default: broker.hivemq.com)"
    )
    parser.add_argument(
        "--broker-port",
        type=int,
        default=1883,
        help="MQTT broker port (default: 1883)"
    )
    parser.add_argument(
        "--facility-topic",
        type=str,
        default=DEFAULT_FACILITY_TOPIC,
        help=f"Facility topic (default: {DEFAULT_FACILITY_TOPIC})"
    )
    parser.add_argument(
        "--market-topic",
        type=str,
        default=DEFAULT_MARKET_TOPIC,
        help=f"Market topic (default: {DEFAULT_MARKET_TOPIC})"
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="MQTT QoS level (default: 0)"
    )
    args = parser.parse_args()
    
    try:
        logger.info("Starting MQTT publisher (facility + market)")
        logger.info(f"Facility CSV: {args.facility_csv}")
        logger.info(f"Market CSV: {args.market_csv}")
        logger.info(f"Facility Topic: {args.facility_topic}")
        logger.info(f"Market Topic: {args.market_topic}")
        
        # Load data
        facility_iter = load_facility_data(args.facility_csv)
        market_iter = load_market_data(args.market_csv)
        
        # Publish in pairs
        publish_paired_streams(
            facility_iter=facility_iter,
            market_iter=market_iter,
            broker_host=args.broker_host,
            broker_port=args.broker_port,
            facility_topic=args.facility_topic,
            market_topic=args.market_topic,
            qos=args.qos,
        )
        
        logger.info("Execution succeeded")
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
        logger.error(f"MQTT error: {e}")
        print(f"Error: MQTT operation failed - {e}")
        return 1
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
