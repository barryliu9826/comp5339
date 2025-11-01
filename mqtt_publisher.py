#!/usr/bin/env python3
"""
MQTT Publisher
功能：按事件时间排序，将 facility_metrics_wide.csv 逐条以 1s 间隔发布到 MQTT。
约束：无状态持久化、无重试/回退；失败直接抛出并退出。
"""

from __future__ import annotations

import json
import os
import time
import argparse
import logging
from typing import List
import pandas as pd
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def publish_csv_to_mqtt(
    csv_path: str,
    broker_host: str = "broker.hivemq.com",
    broker_port: int = 1883,
    topic: str = "a02/facility_metrics/v1/stream",
    qos: int = 0,
    time_column: str = "timestamp",
    facility_column: str = "facility_code",
    power_column: str = "power",
    emissions_column: str = "emissions",
) -> None:
    """
    将CSV行按事件时间排序，以1秒间隔发布到MQTT
    
    适配 facility_metrics_wide.csv 格式：
    - facility_code: 设施代码
    - timestamp: 时间戳（ISO 8601格式，带时区）
    - power: 功率值（MW）
    - emissions: 排放值（t）
    - longitude: 经度
    - latitude: 纬度

    Args:
        csv_path: CSV文件路径（默认: data/csv_output/facility_metrics_wide.csv）
        broker_host: MQTT代理主机名
        broker_port: MQTT代理端口
        topic: MQTT主题
        qos: MQTT QoS级别
        time_column: 时间列名（默认: timestamp）
        facility_column: 设施代码列名（默认: facility_code）
        power_column: 功率列名（默认: power）
        emissions_column: 排放列名（默认: emissions）

    Raises:
        FileNotFoundError: CSV文件不存在
        ValueError: 必需列缺失或时间解析失败
        RuntimeError: MQTT连接/发布失败
    """
    logger.info(
        f"MQTT publish start: host={broker_host}, port={broker_port}, topic={topic}, qos={qos}"
    )

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    required_cols: List[str] = [time_column, facility_column, power_column, emissions_column]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    try:
        # 解析时间戳（支持ISO 8601格式，包括时区）
        df[time_column] = pd.to_datetime(df[time_column], utc=True)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"Failed to parse time column '{time_column}': {e}")

    df = df.sort_values(by=[time_column, facility_column], kind="stable").reset_index(drop=True)

    client = mqtt.Client(protocol=mqtt.MQTTv311)
    rc = client.connect(broker_host, broker_port, keepalive=60)
    if rc != 0:
        raise RuntimeError(f"MQTT connect failed, rc={rc}")

    for idx, row in df.iterrows():
        # 构建MQTT消息负载（匹配facility_metrics_wide.csv格式）
        payload = {
            "event_time": row[time_column].isoformat(),
            "facility_id": str(row[facility_column]),
            "power": float(row[power_column]),
            "emissions": float(row[emissions_column]),
        }
        # 如果存在地理位置信息，也包含在内
        if "longitude" in df.columns and "latitude" in df.columns:
            if pd.notna(row["longitude"]) and pd.notna(row["latitude"]):
                payload["longitude"] = float(row["longitude"])
                payload["latitude"] = float(row["latitude"])
        
        data = json.dumps(payload, ensure_ascii=False)

        info = client.publish(topic, payload=data, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed, rc={info.rc}, index={idx}")

        logger.info(
            f"published index={idx} facility_id={payload['facility_id']} event_time={payload['event_time']} "
            f"power={payload['power']} emissions={payload['emissions']}"
        )
        time.sleep(1.0)  # 改为1秒间隔（原来是0.1秒）

    client.disconnect()
    logger.info("MQTT publish finished")


def main() -> int:
    """
    主函数：解析命令行参数并发布CSV数据到MQTT
    
    默认CSV路径: data/csv_output/facility_metrics_wide.csv
    """
    parser = argparse.ArgumentParser(
        description="将 facility_metrics_wide.csv 发布到 MQTT（1秒间隔）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python mqtt_publisher.py --csv-path data/csv_output/facility_metrics_wide.csv
  python mqtt_publisher.py --csv-path data/csv_output/facility_metrics_wide.csv --broker-host localhost --broker-port 1883
        """
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default="data/csv_output/facility_metrics_wide.csv",
        help="CSV文件路径（默认: data/csv_output/facility_metrics_wide.csv）"
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
        "--topic",
        type=str,
        default="a02/facility_metrics/v1/stream",
        help="MQTT主题（默认: a02/facility_metrics/v1/stream）"
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
        publish_csv_to_mqtt(
            csv_path=args.csv_path,
            broker_host=args.broker_host,
            broker_port=args.broker_port,
            topic=args.topic,
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
    except Exception as e:  # noqa: BLE001
        logger.error(f"程序执行失败: {e}")
        print(f"✗ 错误: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
