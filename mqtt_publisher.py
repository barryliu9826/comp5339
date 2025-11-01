#!/usr/bin/env python3
"""
MQTT Publisher
功能：同时将 facility_metrics_wide.csv 和 market_metrics_data.csv 成对发布到 MQTT。
约束：无状态持久化、无重试/回退；失败直接抛出并退出。
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

# 默认路径
DEFAULT_FACILITY_CSV = "data/csv_output/facility_metrics_wide.csv"
DEFAULT_MARKET_CSV = "data/csv_output/market_metrics_data.csv"
DEFAULT_FACILITY_TOPIC = "a02/facility_metrics/v1/stream"
DEFAULT_MARKET_TOPIC = "a02/market_metrics/v1/stream"


def load_facility_data(csv_path: str) -> Iterator[dict]:
    """
    加载 facility CSV 数据并转换为迭代器.
    
    Args:
        csv_path: CSV文件路径
    
    Yields:
        每条记录作为字典，包含 event_time, facility_id, power, emissions 等
    
    Raises:
        FileNotFoundError: CSV文件不存在
        ValueError: 必需列缺失或时间解析失败
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Facility CSV not found: {csv_path}")
    
    logger.info(f"Loading facility data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # 验证必需列
    required_cols = ["timestamp", "facility_code", "power", "emissions"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in facility CSV: {col}")
    
    # 解析时间戳
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise ValueError(f"Failed to parse timestamp column in facility CSV: {e}")
    
    # 排序
    df = df.sort_values(by=["timestamp", "facility_code"], kind="stable").reset_index(drop=True)
    
    logger.info(f"Loaded {len(df)} facility records")
    
    # 转换为迭代器
    for _, row in df.iterrows():
        payload = {
            "event_time": row["timestamp"].isoformat(),
            "facility_id": str(row["facility_code"]),
            "power": float(row["power"]),
            "emissions": float(row["emissions"]),
        }
        
        # 添加地理位置信息（如果存在）
        if "longitude" in df.columns and "latitude" in df.columns:
            if pd.notna(row["longitude"]) and pd.notna(row["latitude"]):
                payload["longitude"] = float(row["longitude"])
                payload["latitude"] = float(row["latitude"])
        
        yield payload


def load_market_data(csv_path: str) -> Iterator[dict]:
    """
    加载 market CSV 数据并转换为迭代器.
    
    Args:
        csv_path: CSV文件路径
    
    Yields:
        每条记录作为字典，包含 network_code, metric, event_time, value
    
    Raises:
        FileNotFoundError: CSV文件不存在
        ValueError: 必需列缺失或时间解析失败
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Market CSV not found: {csv_path}")
    
    logger.info(f"Loading market data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # 验证必需列
    required_cols = ["network_code", "metric", "timestamp", "value"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in market CSV: {col}")
    
    # 解析时间戳
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise ValueError(f"Failed to parse timestamp column in market CSV: {e}")
    
    # 排序
    df = df.sort_values(by=["timestamp", "network_code", "metric"], kind="stable").reset_index(drop=True)
    
    logger.info(f"Loaded {len(df)} market records")
    
    # 转换为迭代器
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
    成对发布 facility 和 market 数据到 MQTT.
    
    facility 和 market 消息之间零间隔，每对消息后等待1秒。
    当 market 数据用完后，继续单独发布剩余的 facility 数据。
    
    Args:
        facility_iter: Facility 数据迭代器
        market_iter: Market 数据迭代器
        broker_host: MQTT broker 主机名
        broker_port: MQTT broker 端口
        facility_topic: Facility 数据 topic
        market_topic: Market 数据 topic
        qos: MQTT QoS 级别
    Raises:
        RuntimeError: MQTT 连接或发布失败
    """
    logger.info(
        f"Connecting to MQTT broker: {broker_host}:{broker_port}"
    )
    
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    rc = client.connect(broker_host, broker_port, keepalive=60)
    if rc != 0:
        raise RuntimeError(f"MQTT connect failed, rc={rc}")
    
    logger.info("Connected to MQTT broker")
    
    facility_count = 0
    market_count = 0
    pair_count = 0
    
    # 阶段1：配对发布（facility + market，零间隔）
    logger.info("Starting paired publishing phase...")
    
    try:
        while True:
            # 获取下一条 facility 和 market 数据
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
            
            # 如果两者都不存在，退出配对阶段
            if facility_record is None and market_record is None:
                break
            
            # 发布 facility 消息（如果存在）
            if facility_record is not None:
                facility_payload = json.dumps(facility_record, ensure_ascii=False)
                info = client.publish(facility_topic, payload=facility_payload, qos=qos)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    logger.warning(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
                else:
                    facility_count += 1
                    logger.debug(
                        f"Published facility [{facility_count}]: "
                        f"facility_id={facility_record['facility_id']} "
                        f"event_time={facility_record['event_time']}"
                    )
            
            # 立即发布 market 消息（如果存在，零间隔）
            if market_record is not None:
                market_payload = json.dumps(market_record, ensure_ascii=False)
                info = client.publish(market_topic, payload=market_payload, qos=qos)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    logger.warning(f"Failed to publish market message [{market_count}]: rc={info.rc}")
                else:
                    market_count += 1
                    logger.debug(
                        f"Published market [{market_count}]: "
                        f"network_code={market_record['network_code']} "
                        f"metric={market_record['metric']} "
                        f"event_time={market_record['event_time']} "
                        f"value={market_record['value']}"
                    )
            
            pair_count += 1
            
            # 每对消息后等待1秒
            time.sleep(1.0)
            
            # 每100对输出一次进度
            if pair_count % 100 == 0:
                logger.info(
                    f"Progress: {pair_count} pairs published "
                    f"({facility_count} facility, {market_count} market)"
                )
    
    except Exception as e:
        logger.error(f"Error during paired publishing: {e}", exc_info=True)
        raise
    
    # 阶段2：继续发布剩余的 facility 数据（如果 market 先耗尽）
    remaining_facility_count = 0
    
    # 检查是否还有未发布的 facility_record（在最后一次配对中获取的）
    if facility_record is not None:
        logger.info("Market data exhausted. Continuing with remaining facility data...")
        facility_payload = json.dumps(facility_record, ensure_ascii=False)
        info = client.publish(facility_topic, payload=facility_payload, qos=qos)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            facility_count += 1
            remaining_facility_count += 1
            logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
        time.sleep(1.0)
    
    # 继续发布剩余的 facility 数据
    for facility_record in facility_iter:
        facility_payload = json.dumps(facility_record, ensure_ascii=False)
        info = client.publish(facility_topic, payload=facility_payload, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(f"Failed to publish facility message [{facility_count}]: rc={info.rc}")
        else:
            facility_count += 1
            remaining_facility_count += 1
            logger.debug(
                f"Published facility [{facility_count}]: "
                f"facility_id={facility_record['facility_id']} "
                f"event_time={facility_record['event_time']}"
            )
        
        time.sleep(1.0)
        
        # 每100条输出一次进度
        if remaining_facility_count % 100 == 0:
            logger.info(
                f"Remaining facility progress: {remaining_facility_count} published "
                f"(total facility: {facility_count})"
            )
    
    client.disconnect()
    
    logger.info("MQTT publishing finished")
    logger.info(
        f"Total published: {facility_count} facility + {market_count} market = "
        f"{facility_count + market_count} messages"
    )


def main() -> int:
    """
    主函数：加载并成对发布 facility 和 market 数据到 MQTT.
    
    Returns:
        退出码（0表示成功，非0表示失败）
    """
    parser = argparse.ArgumentParser(
        description="同时将 facility 和 market CSV 数据成对发布到 MQTT（1秒间隔）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认路径和配置
  python mqtt_publisher.py
  
  # 指定 broker
  python mqtt_publisher.py --broker-host broker.hivemq.com --broker-port 1883
        """
    )
    parser.add_argument(
        "--facility-csv",
        type=str,
        default=DEFAULT_FACILITY_CSV,
        help=f"Facility CSV文件路径（默认: {DEFAULT_FACILITY_CSV}）"
    )
    parser.add_argument(
        "--market-csv",
        type=str,
        default=DEFAULT_MARKET_CSV,
        help=f"Market CSV文件路径（默认: {DEFAULT_MARKET_CSV}）"
    )
    parser.add_argument(
        "--broker-host",
        type=str,
        default="broker.hivemq.com",
        help="MQTT代理主机名（默认: broker.hivemq.com）"
    )
    parser.add_argument(
        "--broker-port",
        type=int,
        default=1883,
        help="MQTT代理端口（默认: 1883）"
    )
    parser.add_argument(
        "--facility-topic",
        type=str,
        default=DEFAULT_FACILITY_TOPIC,
        help=f"Facility数据topic（默认: {DEFAULT_FACILITY_TOPIC}）"
    )
    parser.add_argument(
        "--market-topic",
        type=str,
        default=DEFAULT_MARKET_TOPIC,
        help=f"Market数据topic（默认: {DEFAULT_MARKET_TOPIC}）"
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="MQTT QoS级别（默认: 0）"
    )
    args = parser.parse_args()
    
    try:
        logger.info("Starting MQTT publisher (facility + market)")
        logger.info(f"Facility CSV: {args.facility_csv}")
        logger.info(f"Market CSV: {args.market_csv}")
        logger.info(f"Facility Topic: {args.facility_topic}")
        logger.info(f"Market Topic: {args.market_topic}")
        
        # 加载数据
        facility_iter = load_facility_data(args.facility_csv)
        market_iter = load_market_data(args.market_csv)
        
        # 成对发布
        publish_paired_streams(
            facility_iter=facility_iter,
            market_iter=market_iter,
            broker_host=args.broker_host,
            broker_port=args.broker_port,
            facility_topic=args.facility_topic,
            market_topic=args.market_topic,
            qos=args.qos,
        )
        
        logger.info("程序执行成功")
        return 0
    
    except FileNotFoundError as e:
        logger.error(f"文件未找到: {e}")
        print(f"✗ 错误: 文件未找到 - {e}")
        return 1
    except ValueError as e:
        logger.error(f"数据格式错误: {e}")
        print(f"✗ 错误: 数据格式错误 - {e}")
        return 1
    except RuntimeError as e:
        logger.error(f"MQTT错误: {e}")
        print(f"✗ 错误: MQTT操作失败 - {e}")
        return 1
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"✗ 错误: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
