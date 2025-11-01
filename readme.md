# MQTT Publisher 使用说明

## 功能说明

`mqtt_publisher.py` 现在默认同时发布 **facility** 和 **market** 数据到 MQTT。

- **成对发布**：每次循环发布一对消息（facility + market），两者之间零间隔
- **批次间隔**：每对消息后等待 1 秒
- **自动处理**：当 market 数据用完后，继续单独发布剩余的 facility 数据

## 默认配置

- **Facility CSV**: `data/csv_output/facility_metrics_wide.csv`
- **Market CSV**: `data/csv_output/market_metrics_data.csv`
- **Facility Topic**: `a02/facility_metrics/v1/stream`
- **Market Topic**: `a02/market_metrics/v1/stream`
- **Broker**: `broker.hivemq.com:1883`
- **QoS**: `0`

## 基本使用

### 使用默认配置（推荐）

```bash
python /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/mqtt_publisher.py
```

### 自定义配置

```bash
python /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/mqtt_publisher.py \
  --broker-host broker.hivemq.com \
  --broker-port 1883 \
  --qos 0
```

### 指定 CSV 路径

```bash
python /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/mqtt_publisher.py \
  --facility-csv /path/to/facility_metrics_wide.csv \
  --market-csv /path/to/market_metrics_data.csv
```

### 自定义 Topic

```bash
python /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/mqtt_publisher.py \
  --facility-topic a02/facility_metrics/v1/stream \
  --market-topic a02/market_metrics/v1/stream
```

## 完整示例

```bash
python /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/mqtt_publisher.py \
  --facility-csv /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/data/csv_output/facility_metrics_wide.csv \
  --market-csv /Users/barry/Desktop/CS/25S2/5339/assignment02_1107/data/csv_output/market_metrics_data.csv \
  --broker-host broker.hivemq.com \
  --broker-port 1883 \
  --facility-topic a02/facility_metrics/v1/stream \
  --market-topic a02/market_metrics/v1/stream \
  --qos 0
```

## 发布时序

```
T=0.0s:  发布 Facility Data 1
T=0.0s:  发布 Market Data 1   (零间隔)
T=1.0s:  (等待 1 秒)
T=1.0s:  发布 Facility Data 2
T=1.0s:  发布 Market Data 2   (零间隔)
T=2.0s:  (等待 1 秒)
...
```

## 查看帮助

```bash
python mqtt_publisher.py --help
```
